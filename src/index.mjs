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
 *
 * Partial runs: `run(id, { follow: false })` / `runAll({ ids, follow: false })`
 * hold the run's dependents (they are treated like manual nodes for that run:
 * flagged `stale`, not run). Equality cut-off: a `run` that returns
 * `UNCHANGED` (the caller compared its own values) settles `done` without
 * rerunning dependents that already saw that value.
 *
 * Live nodes (opt-in): a `run` that returns `live(iterable)`, is itself an
 * `async function*`, or calls `ctx.emit`, settles more than once (each item: `version` +1, dependents run). The state
 * carries `live: true` while the run is open; the status keeps its meaning. An
 * iterable is pulled only once everything downstream of the previous item has
 * settled (backpressure). Superseding, resetting or deleting the node aborts
 * the signal and calls the iterable's `return()`. A node set with
 * `resume: true` isn't restarted by input changes while it is live: the run is
 * told through `ctx.changes` / `ctx.onChange` instead.
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

/**
 * Return this from `run` when the caller decided the node's new value equals
 * its previous one. The node settles `done` with `unchanged: true`, `version`
 * stays, and dependents that already ran on the value aren't rerun or flagged
 * stale. If the node's last settled run wasn't `done` there is nothing to
 * compare with, and it counts as a new value. `Symbol.for`, like `SKIP`.
 */
export const UNCHANGED = Symbol.for("@johnhenry/dataflow.unchanged");

/** Like returning `UNCHANGED`, with a `result` for `state.result` (instead of keeping the previous one). */
export function unchanged(result) {
  return Object.freeze({ [UNCHANGED]: true, result });
}

/** True for `UNCHANGED` and for what `unchanged(result)` returns. */
export function isUnchanged(value) {
  return value === UNCHANGED || (typeof value === "object" && value !== null && value[UNCHANGED] === true);
}

const isAsyncIterable = (value) =>
  value !== null && (typeof value === "object" || typeof value === "function") && typeof value[Symbol.asyncIterator] === "function";

/** Marks what `live(iterable)` returns. `Symbol.for`, like `SKIP`. */
export const LIVE = Symbol.for("@johnhenry/dataflow.live");

/**
 * Return this from `run` to make the run live: each item of `iterable` settles
 * the node. Live runs are opt-in: an async iterable returned without it (a
 * `ReadableStream`, a generator object) is a plain value, as in 0.0.2. A `run`
 * that is itself an `async function*` is live without it.
 */
export function live(iterable) {
  if (!isAsyncIterable(iterable)) throw new TypeError("live: expected an async iterable");
  return Object.freeze({ [LIVE]: true, iterable });
}

/** True for what `live(iterable)` returns. */
export function isLive(value) {
  return typeof value === "object" && value !== null && value[LIVE] === true;
}

// A native `async function*` (also as a method, or bound). A function that
// merely returns a generator object, or a transpiled generator, isn't one.
const isAsyncGeneratorFunction = (fn) => Object.prototype.toString.call(fn) === "[object AsyncGeneratorFunction]";

const DONE = Object.freeze({ value: undefined, done: true });

const normalizeConsumes = (consumesSkip) => (Array.isArray(consumesSkip) ? [...new Set(consumesSkip)] : Boolean(consumesSkip));

/**
 * @param {{
 *   run: (id: string, ctx: { runId: number, signal: AbortSignal, deps: string[], skipped: string[], emit: (result: unknown) => boolean, close: () => void, changes: AsyncIterable<object>, onChange: (fn: (notice: object) => void) => () => void }) => unknown,
 *   onChange?: (id: string, state: object) => void,
 *   schedule?: (fn: () => void) => void,
 * }} options
 */
export function createDataflow({ run, onChange, schedule = queueMicrotask } = {}) {
  if (typeof run !== "function") throw new TypeError("createDataflow: `run` must be a function");
  // `run` itself an async generator function: every run is live, no marker needed.
  const runIsLive = isAsyncGeneratorFunction(run);
  // The iterable a run's result makes live, if any. Anything else is a value.
  const liveIterable = (value) => (isLive(value) ? value.iterable : runIsLive && isAsyncIterable(value) ? value : undefined);

  /**
   * `follow`: how the next run of a queued node treats its dependents (set by
   * `run` / `runAll`, kept until the node starts; absent means follow).
   * `settledDone`: the last settled outcome was `done` (UNCHANGED compares with it).
   * `restart`: an explicit request (`run`, `runAll`, `invalidate`) is queued,
   * so a `resume` node restarts instead of being told about a change.
   * `current`: the run in flight or live (see `start`); cleared when it ends.
   * @type {Map<string, { deps: string[], autorun: boolean, consumesSkip: boolean | string[], resume: boolean, state: any, controller?: AbortController, follow?: boolean, runFollow?: boolean, settledDone?: boolean, restart?: boolean, current?: any }>}
   */
  const nodes = new Map();
  const pending = new Set();
  // Live runs whose last item settled: each pulls its next item once
  // everything downstream of it has settled (see `pump`).
  const pulls = new Set();
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
        stop(node);
        pending.delete(id);
        delete node.follow;
        delete node.restart;
        node.settledDone = false;
        setState(id, {
          status: "error",
          cycle: true,
          live: false,
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
    stop(node);
    // A never-run node that nobody queued stays idle: nobody asked it to run.
    // A queued one was asked, so it waits (and runs once its input recovers).
    if (node.state.status !== "idle" || wasQueued) {
      setState(id, { status: "waiting", runId: node.state.runId + 1, error: undefined, live: false });
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
        stop(node);
        setState(id, { status: "waiting", runId: node.state.runId + 1, error: undefined, live: false });
        for (const down of downstream(id)) markWaiting(down);
        continue;
      }
      const skippedDeps = node.deps.filter((dep) => nodes.get(dep).state.status === "skipped");
      const spreading = skippedDeps.find((dep) => !consumes(node, dep));
      if (spreading !== undefined) {
        const follow = node.follow ?? true;
        delete node.follow;
        delete node.restart;
        spreadSkip(id, nodes.get(spreading).state.skip, follow);
        continue;
      }
      // A live `resume` node is told about the change instead of restarted
      // (an explicit run/runAll/invalidate still restarts it).
      if (node.resume && node.current?.live && !node.restart) {
        notify(node.current, skippedDeps);
        continue;
      }
      start(id, skippedDeps);
    }
    pump();
  }

  // End a node's run, if any: abort its signal, close its live iterable
  // (`return()`, so the producer's `finally` runs), drop its late emits and
  // items, and end its `ctx.changes`. Every path that bumps `runId` without
  // starting a run goes through here, as does `start` and `delete`.
  function stop(node) {
    node.controller?.abort();
    node.controller = undefined;
    const r = node.current;
    if (!r) return;
    node.current = undefined;
    end(r);
    closeIterator(r);
  }

  // The run is over (settled for good, closed, or stopped): nothing it does
  // later counts.
  function end(r) {
    r.ended = true;
    pulls.delete(r);
    r.notice = undefined;
    r.listeners.clear();
    for (const waiter of r.waiters.splice(0)) waiter.resolve(DONE);
  }

  // Close a live iterable. A producer suspended at `yield` runs its `finally`
  // now; one awaiting something runs it when it next yields or throws (the
  // aborted signal is how it finds out sooner). Errors from cleanup are
  // dropped, like the rejection of any superseded run.
  function closeIterator(r) {
    const iterator = r.iterator;
    r.iterator = undefined;
    if (iterator) returnQuietly(iterator);
  }
  function returnQuietly(iterator) {
    try {
      const done = typeof iterator.return === "function" ? iterator.return() : undefined;
      if (done && typeof done.then === "function") done.then(undefined, () => {});
    } catch {
      // dropped
    }
  }

  // The run in flight or live for its node (not superseded, reset, deleted or ended).
  const isCurrent = (r) => {
    if (r.ended) return false;
    const node = nodes.get(r.id);
    return node !== undefined && node.current === r && node.state.runId === r.runId;
  };

  function start(id, skipped = []) {
    const node = nodes.get(id);
    stop(node);
    const controller = new AbortController();
    node.controller = controller;
    const runId = node.state.runId + 1;
    node.runFollow = node.follow ?? true;
    delete node.follow;
    delete node.restart;
    /** One run: settles once (a value), or many times (live: emits and/or iterable items). */
    const r = {
      id,
      runId,
      follow: node.runFollow,
      ended: false, // superseded, reset, deleted, or finished
      closed: false, // ctx.close() was called: no more emits or items
      live: false, // has emitted, or returned an async iterable
      settled: false, // settled at least once (an emit or an item)
      iterator: undefined,
      changed: new Set(), // inputs that settled since the last change notice
      notice: undefined, // the coalesced notice nobody has taken yet
      waiters: [], // pending ctx.changes next() calls
      listeners: new Set(), // ctx.onChange callbacks
    };
    node.current = r;
    setState(id, { status: "running", runId, stale: false, unchanged: false, live: false });
    let result;
    try {
      result = Promise.resolve(run(id, { runId, signal: controller.signal, deps: [...node.deps], skipped, ...liveContext(r) }));
    } catch (error) {
      result = Promise.reject(error);
    }
    result.then(
      (value) => {
        const iterable = liveIterable(value);
        if (!isCurrent(r)) {
          // Superseded before it got going: close its live iterable, unread.
          if (iterable) closeReturned(iterable);
          return;
        }
        if (iterable) {
          if (r.closed) {
            closeReturned(iterable);
            if (r.settled) finish(r);
            else settleLast(r, { status: "done", result: undefined, error: undefined });
            return;
          }
          let iterator;
          try {
            iterator = iterable[Symbol.asyncIterator]();
          } catch (error) {
            settleLast(r, { status: "error", error, result: undefined });
            return;
          }
          r.iterator = iterator;
          if (!r.live) {
            r.live = true;
            setState(id, { live: true });
          }
          if (isCurrent(r)) pull(r); // the first item is the run's own result: no backpressure
          return;
        }
        // A run that emitted settled through its emits; returning ends it.
        if (r.settled) {
          finish(r);
          return;
        }
        if (isSkip(value)) settleLast(r, { status: "skipped", result: undefined, error: undefined, skip: Object.freeze({ source: id, reason: value === SKIP ? undefined : value.reason }) });
        else if (isUnchanged(value)) settleUnchangedLast(r, value);
        else settleLast(r, { status: "done", result: value, error: undefined });
      },
      (error) => {
        if (isCurrent(r)) settleLast(r, { status: "error", error, result: undefined });
      },
    );
  }

  const closeReturned = (iterable) => {
    try {
      returnQuietly(iterable[Symbol.asyncIterator]());
    } catch {
      // dropped
    }
  };

  // What a run adds to its context for live use. Present on every run; a
  // non-live, non-resume run simply never uses them.
  function liveContext(r) {
    return {
      emit: (result) => {
        if (!isCurrent(r) || r.closed) return false; // superseded, reset, deleted, closed or ended
        settleItem(r, result);
        return true;
      },
      close: () => {
        if (!isCurrent(r) || r.closed) return;
        r.closed = true;
        if (r.iterator) {
          closeIterator(r);
          if (r.settled) finish(r);
          else settleLast(r, { status: "done", result: undefined, error: undefined });
        } else if (r.settled) {
          finish(r);
        }
        // Otherwise nothing settled yet: the run settles with what it returns, as usual.
      },
      changes: {
        [Symbol.asyncIterator]: () => changesIterator(r),
      },
      onChange: (fn) => {
        if (typeof fn !== "function") throw new TypeError("ctx.onChange: expected a function");
        if (r.ended) return () => false;
        r.listeners.add(fn);
        return () => r.listeners.delete(fn);
      },
    };
  }

  // `ctx.changes`: a mailbox of one, coalesced latest-wins. Notices are kept
  // from the moment the run starts, so a producer that starts listening late
  // (after its first `yield`, say) doesn't miss one. Ends when the run ends.
  function changesIterator(r) {
    let closed = false;
    const iterator = {
      next() {
        if (closed || r.ended) return Promise.resolve(DONE);
        if (r.notice) {
          const value = r.notice;
          r.notice = undefined;
          return Promise.resolve({ value, done: false });
        }
        return new Promise((resolve) => r.waiters.push({ iterator, resolve }));
      },
      return() {
        closed = true;
        for (let i = r.waiters.length - 1; i >= 0; i--) {
          if (r.waiters[i].iterator === iterator) r.waiters.splice(i, 1)[0].resolve(DONE);
        }
        return Promise.resolve(DONE);
      },
      [Symbol.asyncIterator]() {
        return iterator;
      },
    };
    return iterator;
  }

  // Tell a live `resume` run that its inputs changed (instead of restarting it).
  function notify(r, skipped) {
    const node = nodes.get(r.id);
    const notice = Object.freeze({ deps: [...node.deps], skipped: [...skipped], changed: [...r.changed] });
    r.changed.clear();
    for (const fn of [...r.listeners]) {
      try {
        fn(notice);
      } catch (error) {
        report(error);
      }
    }
    if (!isCurrent(r)) return; // a callback ended the run
    const waiter = r.waiters.shift();
    if (waiter) waiter.resolve({ value: notice, done: false });
    else if (r.notice) r.notice = Object.freeze({ ...notice, changed: [...new Set([...r.notice.changed, ...notice.changed])] });
    else r.notice = notice;
  }

  // Pull a live iterable's next item.
  function pull(r) {
    let step;
    try {
      step = Promise.resolve(r.iterator.next());
    } catch (error) {
      step = Promise.reject(error);
    }
    step.then(
      (item) => {
        if (!isCurrent(r) || r.closed) return; // superseded meanwhile: the item is dropped
        if (item === null || typeof item !== "object") {
          r.iterator = undefined;
          settleLast(r, { status: "error", error: new TypeError(`"${r.id}": iterator result ${item} is not an object`), result: undefined });
          return;
        }
        if (item.done) {
          r.iterator = undefined;
          if (r.settled) finish(r);
          else settleLast(r, { status: "done", result: undefined, error: undefined }); // ended without an item
          return;
        }
        settleItem(r, item.value);
        if (isCurrent(r) && r.iterator) {
          pulls.add(r); // pulled again once downstream settled
          queueFlush(); // the flush that ends the wave pumps it
        }
      },
      (error) => {
        if (!isCurrent(r) || r.closed) return;
        r.iterator = undefined;
        settleLast(r, { status: "error", error, result: undefined });
      },
    );
  }

  // Backpressure: a live run pulls its next item once nothing downstream of
  // it is queued or running -- the `upstreamBusy` rule, applied downstream.
  // Runs after every flush, so it sees each wave's end; a paused graph
  // doesn't flush, so it doesn't pull either.
  const downstreamBusy = (id) => {
    for (const down of downstream(id)) {
      if (pending.has(down) || nodes.get(down)?.state.status === "running") return true;
    }
    return false;
  };
  function pump() {
    for (const r of [...pulls]) {
      if (!isCurrent(r) || !r.iterator) {
        pulls.delete(r);
        continue;
      }
      if (downstreamBusy(r.id)) continue;
      pulls.delete(r);
      pull(r);
    }
  }

  // One settle of a live run (an emit or an item); the run goes on.
  function settleItem(r, value) {
    const node = nodes.get(r.id);
    r.live = true;
    r.settled = true;
    if (isSkip(value)) {
      node.settledDone = false;
      setState(r.id, { status: "skipped", result: undefined, error: undefined, unchanged: false, live: true, version: node.state.version + 1, skip: Object.freeze({ source: r.id, reason: value === SKIP ? undefined : value.reason }) });
      propagate(r.id, false, { follow: r.follow });
    } else if (isUnchanged(value) && node.settledDone) {
      const patch = { status: "done", error: undefined, unchanged: true, live: true };
      if (value !== UNCHANGED) patch.result = value.result;
      setState(r.id, patch);
      propagate(r.id, false, { follow: r.follow, unchanged: true });
    } else {
      const result = isUnchanged(value) ? (value === UNCHANGED ? undefined : value.result) : value;
      node.settledDone = true;
      setState(r.id, { status: "done", result, error: undefined, unchanged: false, live: true, version: node.state.version + 1 });
      propagate(r.id, false, { follow: r.follow });
    }
  }

  // A live run ended on its own (its iterable finished, its promise resolved
  // after emitting, or ctx.close()): no new settle, just `live: false`.
  function finish(r) {
    const node = nodes.get(r.id);
    node.controller = undefined;
    node.current = undefined;
    end(r);
    if (node.state.live) setState(r.id, { live: false });
  }

  // The run's last settle: its returned value, its error, or an empty iterable.
  function settleLast(r, patch) {
    const node = nodes.get(r.id);
    node.controller = undefined;
    node.current = undefined;
    end(r);
    node.settledDone = patch.status === "done";
    if (node.state.live) patch = { ...patch, live: false };
    setState(r.id, { ...patch, version: node.state.version + 1 });
    propagate(r.id, patch.status === "error", { follow: node.runFollow });
  }

  // The caller says the value didn't change. Only meaningful after a `done`:
  // otherwise there is nothing to compare with, and it is a new value.
  function settleUnchangedLast(r, value) {
    const node = nodes.get(r.id);
    const result = value === UNCHANGED ? undefined : value.result;
    if (!node.settledDone) {
      settleLast(r, { status: "done", result, error: undefined });
      return;
    }
    node.controller = undefined;
    node.current = undefined;
    end(r);
    const patch = { status: "done", error: undefined, unchanged: true };
    if (value !== UNCHANGED) patch.result = result;
    setState(r.id, patch);
    propagate(r.id, false, { follow: node.runFollow, unchanged: true });
  }

  // An input was skipped and this node doesn't consume it: skip without
  // running. Nothing ran, so `version` stays; `runId` bumps so that a run of
  // this node still in flight is dropped when it settles.
  function spreadSkip(id, from, follow = true) {
    const node = nodes.get(id);
    stop(node);
    node.settledDone = false;
    setState(id, { status: "skipped", runId: node.state.runId + 1, result: undefined, error: undefined, stale: false, unchanged: false, live: false, skip: from });
    propagate(id, false, { follow });
  }

  // A node has a new outcome; tell its dependents.
  //  follow: false   a held run: every dependent is treated like a manual node
  //                  (flagged stale, not run; a waiting or queued one still runs).
  //  unchanged: true the value didn't change: dependents that already ran on
  //                  it (or failed on it) are left alone; waiting and never-run
  //                  autorun ones still run.
  function propagate(id, failed, { follow = true, unchanged = false } = {}) {
    for (const dep of dependents(id)) {
      const d = nodes.get(dep);
      if (!d || d.state.cycle) continue;
      if (unchanged && d.state.status !== "waiting" && d.state.status !== "idle") continue;
      if (failed) {
        markWaiting(dep);
        for (const down of downstream(dep)) markWaiting(down);
      } else if ((follow && d.autorun) || d.state.status === "waiting" || pending.has(dep)) {
        // Autorun nodes follow their inputs (unless the run is held); a waiting
        // node was already asked to run; a queued one runs anyway.
        if (d.resume && d.current?.live) d.current.changed.add(id);
        pending.add(dep);
      } else if (d.state.status === "done" || d.state.status === "skipped") {
        setState(dep, { stale: true });
      }
    }
    queueFlush();
  }

  const api = {
    /** Add a node, or replace its dependency list and autorun flag. Never runs anything by itself. */
    set(id, { deps = [], autorun = true, consumesSkip = false, resume = false } = {}) {
      if (typeof id !== "string" || !id) throw new TypeError("dataflow.set: id must be a non-empty string");
      const unique = [...new Set(deps)];
      const existing = nodes.get(id);
      if (existing) {
        existing.deps = unique;
        existing.autorun = Boolean(autorun);
        existing.consumesSkip = normalizeConsumes(consumesSkip);
        existing.resume = Boolean(resume);
      } else {
        nodes.set(id, {
          deps: unique,
          autorun: Boolean(autorun),
          consumesSkip: normalizeConsumes(consumesSkip),
          resume: Boolean(resume),
          state: { status: "idle", version: 0, runId: 0, stale: false, unchanged: false, cycle: false, live: false },
        });
        emit(id);
      }
      refreshCycles();
      if (pulls.size) queueFlush(); // the graph changed under a live run waiting to pull
      return api;
    },

    /** Remove a node. Its dependents start waiting for an input that no longer exists. */
    delete(id) {
      const node = nodes.get(id);
      if (!node) return api;
      stop(node);
      pending.delete(id);
      const affected = dependents(id);
      nodes.delete(id);
      refreshCycles();
      for (const dep of affected) {
        markWaiting(dep);
        for (const down of downstream(dep)) markWaiting(down);
      }
      if (pulls.size) queueFlush(); // a live run upstream may have been waiting on it
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
    /** The node's `resume` option (named `resumes` because `resume()` un-pauses the graph). */
    resumes: (id) => nodes.get(id)?.resume,
    dependents: (id) => dependents(id),
    order: (ids) => order(ids),
    ids: () => [...nodes.keys()],

    /**
     * Run a node now, whatever its autorun flag. Autorun dependents follow,
     * unless `follow: false`: then they are held (flagged stale, not run).
     */
    run(id, { follow = true } = {}) {
      const node = nodes.get(id);
      if (!node) return api;
      node.follow = Boolean(follow);
      node.restart = true;
      pending.add(id);
      queueFlush();
      return api;
    },

    /** A node's definition changed: autorun nodes rerun, manual nodes are marked stale. */
    invalidate(id) {
      const node = nodes.get(id);
      if (!node || node.state.cycle) return api;
      if (node.autorun) {
        node.restart = true; // a new definition: a live `resume` node restarts too
        pending.add(id);
        queueFlush();
      } else if (node.state.status === "done" || node.state.status === "skipped") {
        setState(id, { stale: true });
      }
      return api;
    },

    /**
     * Queue every node (or `ids`) in dependency order. `all: false` skips
     * manual nodes. `follow: false` holds dependents outside the queued set.
     */
    runAll({ all = true, ids, follow = true } = {}) {
      for (const id of order(ids)) {
        const node = nodes.get(id);
        if (!all && !node.autorun) continue;
        node.follow = Boolean(follow);
        node.restart = true;
        pending.add(id);
      }
      queueFlush();
      return api;
    },

    /** Forget every result (for example after the place values live was restarted). */
    reset() {
      pending.clear();
      for (const [id, node] of nodes) {
        stop(node);
        delete node.follow;
        delete node.restart;
        node.settledDone = false;
        if (node.state.cycle) continue;
        node.state = { ...node.state, status: "idle", result: undefined, error: undefined, stale: false, unchanged: false, live: false, runId: node.state.runId + 1 };
        delete node.state.skip;
        emit(id);
      }
      return api;
    },

    /** Stop starting runs (running ones still settle; live iterables aren't pulled). */
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
