/**
 * @johnhenry/dataflow -- an async dataflow scheduler.
 *
 * Nodes depend on other nodes. An injected async `run(id, ctx)` computes a
 * node; the scheduler decides *when* each node runs and tracks *how* its last
 * run went. It never stores node values itself, so the values can live
 * anywhere: in memory, in a Worker, in an iframe, on a server.
 *
 * States: idle | waiting | running | done | skipped | error
 *   idle     never run, or reset
 *   waiting  asked to run, but an input is missing, errored or not run yet
 *   running  `run` was called and has not settled
 * (A node queued behind upstream work keeps its previous state until it starts.)
 *   done     the last run resolved
 *   skipped  the last run returned SKIP / skip(reason), or an input was skipped
 *            (the skip spreads to dependents without running them, unless they
 *            declared `consumesSkip`)
 *   error    the last run rejected, or the node sits on a dependency cycle
 *
 * Guarantees: a node never runs while anything upstream of it is pending or
 * running (no glitches: a diamond's join runs once per upstream change); a
 * newer run supersedes an older one (the older result is dropped and its
 * AbortSignal fires); errors do not cascade as errors (dependents wait).
 */

/**
 * Return this from `run` to decline to produce a value. The node settles as
 * `skipped`, and the skip spreads to its dependents (they become `skipped`
 * without running) except those that declared `consumesSkip`. `Symbol.for`, so
 * two copies of this package agree on it.
 */
export const SKIP = Symbol.for("@johnhenry/dataflow.skip");

/** Like returning `SKIP`, with a reason that dependents see in `state.skip.reason`. */
export function skip(reason) {
  return Object.freeze({ [SKIP]: true, reason });
}

/** True for `SKIP` and for what `skip(reason)` returns. */
export function isSkip(value) {
  return value === SKIP || (typeof value === "object" && value !== null && value[SKIP] === true);
}

const normalizeConsumes = (consumesSkip) => (Array.isArray(consumesSkip) ? [...new Set(consumesSkip)] : Boolean(consumesSkip));

/**
 * @param {{
 *   run: (id: string, ctx: { runId: number, signal: AbortSignal, deps: string[], skipped: string[] }) => unknown,
 *   onChange?: (id: string, state: object) => void,
 *   schedule?: (fn: () => void) => void,
 * }} options
 */
export function createDataflow({ run, onChange, schedule = queueMicrotask } = {}) {
  if (typeof run !== "function") throw new TypeError("createDataflow: `run` must be a function");

  /** @type {Map<string, { deps: string[], autorun: boolean, consumesSkip: boolean | string[], state: any, controller?: AbortController }>} */
  const nodes = new Map();
  const pending = new Set();
  const listeners = new Set();
  if (onChange) listeners.add(onChange);
  let flushQueued = false;
  let paused = false;

  // Like EventTarget: a listener that throws is reported, and neither the
  // other listeners nor the scheduler are affected.
  const report =
    typeof globalThis.reportError === "function"
      ? (error) => globalThis.reportError(error)
      : (error) => queueMicrotask(() => {
          throw error;
        });
  const emit = (id) => {
    const node = nodes.get(id);
    if (!node) return;
    const snapshot = { ...node.state };
    for (const fn of listeners) {
      try {
        fn(id, snapshot);
      } catch (error) {
        report(error);
      }
    }
  };

  const setState = (id, patch) => {
    const node = nodes.get(id);
    if (!node) return;
    const next = { ...node.state, ...patch };
    if (next.status !== "skipped") delete next.skip; // only a skipped node carries skip info
    node.state = next;
    emit(id);
  };

  const dependents = (id) => {
    const out = [];
    for (const [other, node] of nodes) if (node.deps.includes(id)) out.push(other);
    return out;
  };

  const downstream = (id) => {
    const seen = new Set();
    const stack = dependents(id);
    while (stack.length) {
      const next = stack.pop();
      if (seen.has(next)) continue;
      seen.add(next);
      stack.push(...dependents(next));
    }
    return seen;
  };

  const findCycles = () => {
    const onCycle = new Set();
    const color = new Map(); // undefined white, 1 grey, 2 black
    const path = [];
    const visit = (id) => {
      color.set(id, 1);
      path.push(id);
      for (const dep of nodes.get(id)?.deps ?? []) {
        if (!nodes.has(dep)) continue;
        const c = color.get(dep);
        if (c === 1) for (let i = path.lastIndexOf(dep); i < path.length; i++) onCycle.add(path[i]);
        else if (c === undefined) visit(dep);
      }
      path.pop();
      color.set(id, 2);
    };
    for (const id of nodes.keys()) if (!color.has(id)) visit(id);
    return onCycle;
  };

  const refreshCycles = () => {
    const cyclic = findCycles();
    for (const [id, node] of nodes) {
      if (cyclic.has(id) && !node.state.cycle) {
        node.controller?.abort();
        node.controller = undefined;
        pending.delete(id);
        setState(id, {
          status: "error",
          cycle: true,
          runId: node.state.runId + 1,
          error: new Error(`"${id}" is part of a dependency cycle`),
        });
      } else if (!cyclic.has(id) && node.state.cycle) {
        setState(id, { status: "idle", cycle: false, error: undefined });
      }
    }
  };

  const order = (ids = [...nodes.keys()]) => {
    const wanted = new Set(ids);
    const cyclic = findCycles();
    const out = [];
    const seen = new Set();
    const visit = (id) => {
      if (seen.has(id) || cyclic.has(id) || !nodes.has(id)) return;
      seen.add(id);
      for (const dep of nodes.get(id).deps) visit(dep);
      if (wanted.has(id)) out.push(id);
    };
    for (const id of ids) visit(id);
    return out;
  };

  // An input is available when its last run resolved, or when it was skipped
  // (a skip is an answer too: it either spreads or is consumed).
  const depsReady = (id) =>
    nodes.get(id).deps.every((dep) => {
      const status = nodes.get(dep)?.state.status;
      return status === "done" || status === "skipped";
    });

  const consumes = (node, dep) => node.consumesSkip === true || (Array.isArray(node.consumesSkip) && node.consumesSkip.includes(dep));

  const upstreamBusy = (id) => {
    const seen = new Set();
    const stack = [...(nodes.get(id)?.deps ?? [])];
    while (stack.length) {
      const dep = stack.pop();
      if (seen.has(dep)) continue;
      seen.add(dep);
      const node = nodes.get(dep);
      if (!node) continue;
      if (node.state.status === "running" || pending.has(dep)) return true;
      stack.push(...node.deps);
    }
    return false;
  };

  const markWaiting = (id) => {
    const node = nodes.get(id);
    if (!node || node.state.cycle) return;
    const wasQueued = pending.delete(id);
    node.controller?.abort();
    node.controller = undefined;
    // A never-run node that nobody queued stays idle: nobody asked it to run.
    // A queued one was asked, so it waits (and runs once its input recovers).
    if (node.state.status !== "idle" || wasQueued) {
      setState(id, { status: "waiting", runId: node.state.runId + 1, error: undefined });
    }
  };

  const queueFlush = () => {
    if (flushQueued || paused) return;
    flushQueued = true;
    schedule(flush);
  };

  function flush() {
    flushQueued = false;
    if (paused) return;
    for (const id of [...pending]) {
      const node = nodes.get(id);
      if (!node || node.state.cycle) {
        pending.delete(id);
        continue;
      }
      if (upstreamBusy(id)) continue; // settles later; its completion re-flushes
      pending.delete(id);
      if (!depsReady(id)) {
        node.controller?.abort();
        node.controller = undefined;
        setState(id, { status: "waiting", runId: node.state.runId + 1, error: undefined });
        for (const down of downstream(id)) markWaiting(down);
        continue;
      }
      const skippedDeps = node.deps.filter((dep) => nodes.get(dep).state.status === "skipped");
      const spreading = skippedDeps.find((dep) => !consumes(node, dep));
      if (spreading !== undefined) {
        spreadSkip(id, nodes.get(spreading).state.skip);
        continue;
      }
      start(id, skippedDeps);
    }
  }

  function start(id, skipped = []) {
    const node = nodes.get(id);
    node.controller?.abort();
    const controller = new AbortController();
    node.controller = controller;
    const runId = node.state.runId + 1;
    setState(id, { status: "running", runId, stale: false });
    let result;
    try {
      result = Promise.resolve(run(id, { runId, signal: controller.signal, deps: [...node.deps], skipped }));
    } catch (error) {
      result = Promise.reject(error);
    }
    result.then(
      (value) =>
        isSkip(value)
          ? settle(id, runId, { status: "skipped", result: undefined, error: undefined, skip: Object.freeze({ source: id, reason: value === SKIP ? undefined : value.reason }) })
          : settle(id, runId, { status: "done", result: value, error: undefined }),
      (error) => settle(id, runId, { status: "error", error, result: undefined }),
    );
  }

  function settle(id, runId, patch) {
    const node = nodes.get(id);
    if (!node || node.state.runId !== runId || node.state.status !== "running") return; // superseded
    node.controller = undefined;
    setState(id, { ...patch, version: node.state.version + 1 });
    propagate(id, patch.status === "error");
  }

  // An input was skipped and this node doesn't consume it: skip without
  // running. Nothing ran, so `version` stays; `runId` bumps so that a run of
  // this node still in flight is dropped when it settles.
  function spreadSkip(id, from) {
    const node = nodes.get(id);
    node.controller?.abort();
    node.controller = undefined;
    setState(id, { status: "skipped", runId: node.state.runId + 1, result: undefined, error: undefined, stale: false, skip: from });
    propagate(id, false);
  }

  // A node has a new outcome; tell its dependents.
  function propagate(id, failed) {
    for (const dep of dependents(id)) {
      const d = nodes.get(dep);
      if (!d || d.state.cycle) continue;
      if (failed) {
        markWaiting(dep);
        for (const down of downstream(dep)) markWaiting(down);
      } else if (d.autorun || d.state.status === "waiting") {
        // Autorun nodes follow their inputs; a waiting node was already asked to run.
        pending.add(dep);
      } else if (d.state.status === "done" || d.state.status === "skipped") {
        setState(dep, { stale: true });
      }
    }
    queueFlush();
  }

  const api = {
    /** Add a node, or replace its dependency list and autorun flag. Never runs anything by itself. */
    set(id, { deps = [], autorun = true, consumesSkip = false } = {}) {
      if (typeof id !== "string" || !id) throw new TypeError("dataflow.set: id must be a non-empty string");
      const unique = [...new Set(deps)];
      const existing = nodes.get(id);
      if (existing) {
        existing.deps = unique;
        existing.autorun = Boolean(autorun);
        existing.consumesSkip = normalizeConsumes(consumesSkip);
      } else {
        nodes.set(id, {
          deps: unique,
          autorun: Boolean(autorun),
          consumesSkip: normalizeConsumes(consumesSkip),
          state: { status: "idle", version: 0, runId: 0, stale: false, cycle: false },
        });
        emit(id);
      }
      refreshCycles();
      return api;
    },

    /** Remove a node. Its dependents start waiting for an input that no longer exists. */
    delete(id) {
      const node = nodes.get(id);
      if (!node) return api;
      node.controller?.abort();
      pending.delete(id);
      const affected = dependents(id);
      nodes.delete(id);
      refreshCycles();
      for (const dep of affected) {
        markWaiting(dep);
        for (const down of downstream(dep)) markWaiting(down);
      }
      return api;
    },

    has: (id) => nodes.has(id),
    get: (id) => (nodes.has(id) ? { ...nodes.get(id).state } : undefined),
    deps: (id) => [...(nodes.get(id)?.deps ?? [])],
    autorun: (id) => nodes.get(id)?.autorun,
    consumesSkip: (id) => {
      const value = nodes.get(id)?.consumesSkip;
      return Array.isArray(value) ? [...value] : value;
    },
    dependents: (id) => dependents(id),
    order: (ids) => order(ids),
    ids: () => [...nodes.keys()],

    /** Run a node now, whatever its autorun flag. Autorun dependents follow. */
    run(id) {
      if (!nodes.has(id)) return api;
      pending.add(id);
      queueFlush();
      return api;
    },

    /** A node's definition changed: autorun nodes rerun, manual nodes are marked stale. */
    invalidate(id) {
      const node = nodes.get(id);
      if (!node || node.state.cycle) return api;
      if (node.autorun) {
        pending.add(id);
        queueFlush();
      } else if (node.state.status === "done" || node.state.status === "skipped") {
        setState(id, { stale: true });
      }
      return api;
    },

    /** Queue every node (or `ids`) in dependency order. `all: false` skips manual nodes. */
    runAll({ all = true, ids } = {}) {
      for (const id of order(ids)) if (all || nodes.get(id).autorun) pending.add(id);
      queueFlush();
      return api;
    },

    /** Forget every result (for example after the place values live was restarted). */
    reset() {
      pending.clear();
      for (const [id, node] of nodes) {
        node.controller?.abort();
        node.controller = undefined;
        if (node.state.cycle) continue;
        node.state = { ...node.state, status: "idle", result: undefined, error: undefined, stale: false, runId: node.state.runId + 1 };
        delete node.state.skip;
        emit(id);
      }
      return api;
    },

    /** Stop starting runs (running ones still settle). */
    pause() {
      paused = true;
      return api;
    },
    /** Start runs again, flushing anything queued meanwhile. */
    resume() {
      paused = false;
      queueFlush();
      return api;
    },
    get paused() {
      return paused;
    },

    /** Listen to every state change. Returns an unsubscribe function. */
    subscribe(fn) {
      listeners.add(fn);
      return () => listeners.delete(fn);
    },

    /** Resolves once nothing is pending or running (or the scheduler is paused). */
    async idle() {
      for (;;) {
        await new Promise((resolve) => setTimeout(resolve, 0));
        const busy = pending.size > 0 || [...nodes.values()].some((n) => n.state.status === "running");
        if (!busy || paused) return;
      }
    },
  };
  return api;
}

export default createDataflow;
