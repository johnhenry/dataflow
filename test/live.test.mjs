import { test } from "node:test";
import assert from "node:assert/strict";
import { createDataflow, SKIP, skip, UNCHANGED } from "../src/index.mjs";

// No wall-clock sleeps: a "tick" is one turn of the event loop, and producers
// are driven by hand through deferreds.
const tick = () => new Promise((resolve) => setImmediate(resolve));
const ticks = async (n) => {
  for (let i = 0; i < n; i++) await tick();
};
function deferred() {
  let resolve, reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

/**
 * A producer driven by hand: `push(v)` hands it the next value, `log` records
 * what happened (pulls, cleanup). It yields what it is pushed, in order. Like
 * any well-behaved producer it stops waiting when its signal aborts (a
 * `return()` only reaches a generator at a `yield`).
 */
function source(signal) {
  const queue = [];
  let wake;
  const log = { pulls: 0, cleaned: false, aborted: false };
  async function* gen() {
    try {
      for (;;) {
        while (!queue.length) {
          const d = deferred();
          wake = d.resolve;
          signal?.addEventListener("abort", d.resolve, { once: true });
          await d.promise;
          if (signal?.aborted) return;
        }
        log.pulls++;
        yield queue.shift();
      }
    } finally {
      log.cleaned = true;
      log.aborted = signal?.aborted ?? false;
    }
  }
  return {
    iterable: gen(),
    log,
    push(...values) {
      queue.push(...values);
      wake?.();
    },
  };
}

/** Values live outside the scheduler; `defs` maps id -> (inputs..., ctx) => value | async iterable. */
function sheet() {
  const values = new Map();
  const defs = new Map();
  const runs = [];
  const states = [];
  const flow = createDataflow({
    run(id, ctx) {
      runs.push(id);
      return defs.get(id)(...ctx.deps.map((d) => values.get(d)), ctx);
    },
    onChange: (id, s) => states.push([id, s]),
  });
  const define = (id, deps, fn, opts = {}) => {
    defs.set(id, fn);
    flow.set(id, { deps, ...opts });
  };
  /** Wrap a plain formula so its value is stored (as a caller's `run` would). */
  const store = (id, fn) => async (...args) => {
    const ctx = args.at(-1);
    const value = await fn(...args.slice(0, -1));
    if (!ctx.signal.aborted) values.set(id, value);
    return value;
  };
  return { flow, values, defs, runs, states, define, store };
}

test("a run that returns an async iterable settles once per item: version +1, live while open", async () => {
  const { flow, define, states } = sheet();
  const src = source();
  define("live", [], () => src.iterable);
  flow.run("live");
  await ticks(2); // running, nothing yielded yet: idle() would wait for it
  assert.deepEqual(flow.get("live"), { status: "running", version: 0, runId: 1, stale: false, unchanged: false, cycle: false, live: true });
  src.push("x");
  await flow.idle();
  assert.equal(flow.get("live").status, "done");
  assert.equal(flow.get("live").result, "x");
  assert.equal(flow.get("live").version, 1);
  assert.equal(flow.get("live").live, true);
  src.push("y", "z");
  await flow.idle();
  assert.equal(flow.get("live").result, "z");
  assert.equal(flow.get("live").version, 3);
  assert.equal(flow.get("live").runId, 1, "one run, many settles");
  // onChange saw every settle, with live: true.
  const seen = states.filter(([id]) => id === "live").map(([, s]) => `${s.status}:${s.version}:${s.live}`);
  assert.deepEqual(seen, ["idle:0:false", "running:0:false", "running:0:true", "done:1:true", "done:2:true", "done:3:true"]);
});

test("an iterable that finishes ends the live run: live false, status and result kept", async () => {
  const { flow, define, states } = sheet();
  define("n", [], async function* () {
    yield 1;
    yield 2;
  });
  flow.run("n");
  await flow.idle();
  assert.deepEqual(flow.get("n"), { status: "done", version: 2, runId: 1, stale: false, unchanged: false, cycle: false, live: false, result: 2, error: undefined });
  assert.equal(states.at(-1)[1].live, false);
  // An iterable that ends without an item settles done with nothing, like a function that returns nothing.
  define("empty", [], async function* () {});
  flow.run("empty");
  await flow.idle();
  assert.equal(flow.get("empty").status, "done");
  assert.equal(flow.get("empty").version, 1);
  assert.equal(flow.get("empty").result, undefined);
  assert.equal(flow.get("empty").live, false);
});

test("live items: UNCHANGED keeps version and dependents; SKIP settles skipped and spreads; a later item recovers", async () => {
  const { flow, define, store, values, runs } = sheet();
  const src = source();
  define("live", [], () => src.iterable);
  define("dep", ["live"], store("dep", (v) => `seen ${v}`));
  flow.run("live");
  src.push(1);
  await flow.idle();
  assert.equal(values.get("dep"), "seen undefined", "the live source stores nothing; dep ran once");
  runs.length = 0;
  src.push(UNCHANGED);
  await flow.idle();
  assert.equal(flow.get("live").unchanged, true);
  assert.equal(flow.get("live").version, 1);
  assert.equal(flow.get("live").result, 1, "a bare UNCHANGED keeps the previous result");
  assert.deepEqual(runs, [], "dependents weren't rerun");
  src.push(skip("quiet"));
  await flow.idle();
  assert.equal(flow.get("live").status, "skipped");
  assert.equal(flow.get("live").unchanged, false);
  assert.equal(flow.get("live").version, 2);
  assert.deepEqual(flow.get("dep").skip, { source: "live", reason: "quiet" });
  assert.equal(flow.get("dep").status, "skipped");
  src.push(SKIP);
  await flow.idle();
  assert.equal(flow.get("live").version, 3);
  src.push(5);
  await flow.idle();
  assert.equal(flow.get("live").status, "done");
  assert.equal(flow.get("live").live, true);
  assert.equal(flow.get("dep").status, "done");
  assert.deepEqual(runs, ["dep"]);
});

test("an error from the iterable settles error, ends the live run, and dependents wait", async () => {
  const { flow, define, store } = sheet();
  define("live", [], async function* () {
    yield 1;
    throw new Error("socket closed");
  });
  define("dep", ["live"], store("dep", (v) => v));
  flow.run("live");
  await flow.idle();
  assert.equal(flow.get("live").status, "error");
  assert.equal(flow.get("live").error.message, "socket closed");
  assert.equal(flow.get("live").live, false);
  assert.equal(flow.get("live").version, 2);
  assert.equal(flow.get("dep").status, "waiting");
});

test("the diamond with a live source: the join runs once per item, on one consistent version", async () => {
  const { flow, define, values } = sheet();
  const src = source();
  const joins = [];
  define("a", [], async function* () {
    for await (const v of src.iterable) {
      values.set("a", v);
      yield v;
    }
  });
  // Different speeds, so a glitch would show.
  define("b", ["a"], async (a, ctx) => {
    await ticks(3);
    if (!ctx.signal.aborted) values.set("b", a + 1);
    return a + 1;
  });
  define("c", ["a"], async (a, ctx) => {
    await ticks(1);
    if (!ctx.signal.aborted) values.set("c", a * 10);
    return a * 10;
  });
  define("d", ["b", "c"], (b, c) => {
    joins.push([b, c]);
    return b + c;
  });
  flow.runAll();
  src.push(1, 2, 3);
  await flow.idle();
  assert.deepEqual(joins, [
    [2, 10],
    [3, 20],
    [4, 30],
  ]);
  assert.equal(flow.get("a").version, 3);
  assert.equal(flow.get("d").version, 3);
});

test("the diamond with an emitting source: emits supersede the wave in flight, and the join never mixes versions", async () => {
  const { flow, define, values } = sheet();
  const joins = [];
  let emit;
  define("a", [], (ctx) => {
    emit = (v) => (values.set("a", v), ctx.emit(v));
    emit(1);
    return new Promise(() => {});
  });
  define("b", ["a"], async (a, ctx) => {
    await ticks(3);
    if (!ctx.signal.aborted) values.set("b", a);
    return a;
  });
  define("c", ["a"], async (a, ctx) => {
    await ticks(1);
    if (!ctx.signal.aborted) values.set("c", a);
    return a;
  });
  define("d", ["b", "c"], (b, c) => {
    joins.push([b, c]);
  });
  flow.runAll();
  await flow.idle();
  emit(2);
  await ticks(2); // c settled 2, b still running on 2
  emit(3); // supersedes b and c
  await ticks(1);
  emit(4);
  await flow.idle();
  assert.deepEqual(joins.at(-1), [4, 4]);
  assert.ok(joins.every(([b, c]) => b === c), `no mixed versions: ${JSON.stringify(joins)}`);
  assert.equal(flow.get("a").version, 4);
  flow.reset();
});

test("backpressure: a fast producer is pulled only after everything downstream of the last item settled", async () => {
  const { flow, define } = sheet();
  const events = [];
  const running = new Set();
  const N = 20;
  define("fast", [], async function* () {
    for (let i = 1; i <= N; i++) {
      events.push(`pull ${i} busy=${[...running].join(",")}`);
      yield i;
    }
  });
  const slow = (id, n) => async (_input, ctx) => {
    running.add(id);
    await ticks(n);
    running.delete(id);
    events.push(`${id} ${ctx.signal.aborted ? "aborted" : "ok"}`);
  };
  define("b", ["fast"], slow("b", 2));
  define("c", ["b"], slow("c", 3)); // transitive: the producer waits for it too
  define("d", ["fast"], slow("d", 1));
  flow.run("fast");
  await flow.idle();
  const pulls = events.filter((e) => e.startsWith("pull"));
  assert.equal(pulls.length, N);
  assert.ok(pulls.every((e) => e.endsWith("busy=")), `nothing downstream was running at a pull: ${pulls}`);
  assert.ok(!events.some((e) => e.endsWith("aborted")), "no dependent run was superseded");
  assert.equal(events.filter((e) => e === "c ok").length, N, "the whole downstream ran once per item");
  // Pull k+1 comes after c finished for item k.
  for (let i = 1; i < N; i++) {
    const nextPull = events.findIndex((e) => e.startsWith(`pull ${i + 1} `));
    const cDone = events.reduce((at, e, j) => (e === "c ok" && events.slice(0, j).filter((x) => x.startsWith("pull")).length === i ? j : at), -1);
    assert.ok(cDone >= 0 && cDone < nextPull, `item ${i}: c settled before pull ${i + 1}`);
  }
  assert.equal(flow.get("fast").version, N);
  assert.equal(flow.get("fast").live, false);
});

test("backpressure counts only queued and running nodes: manual, stale (held), waiting and errored dependents don't hold the producer", async () => {
  const { flow, define, runs } = sheet();
  let pulls = 0;
  const gate = deferred();
  define("broken", [], () => {
    throw new Error("broken");
  });
  define("fast", [], async function* () {
    for (let i = 1; i <= 5; i++) {
      pulls++;
      yield i;
    }
    await gate.promise;
  });
  define("manual", ["fast"], () => "m", { autorun: false });
  define("waits", ["fast", "broken"], () => "w");
  flow.runAll();
  await flow.idle();
  assert.equal(pulls, 5, "every item pulled although no dependent ran on it");
  assert.equal(flow.get("manual").stale, true, "the manual dependent was flagged stale, as for any run");
  assert.equal(flow.get("waits").status, "waiting");
  assert.equal(runs.filter((id) => id === "manual").length, 1, "the manual node ran once (runAll), not per item");
  gate.resolve();
  await flow.idle();
  assert.equal(flow.get("fast").live, false);
});

test("a live dependent counts as settled once it settled once; until then it holds the source", async () => {
  const { flow, define } = sheet();
  const events = [];
  let gate = deferred();
  define("src", [], async function* () {
    for (let i = 1; i <= 2; i++) {
      events.push(`pull ${i}`);
      yield i;
    }
  });
  define("inner", ["src"], async function* () {
    await gate.promise;
    events.push("inner first");
    yield "first";
    await new Promise(() => {}); // stays live, never settles again
  });
  flow.run("src");
  await ticks(5); // "inner" is running: idle() would wait for it
  assert.deepEqual(events, ["pull 1"], "held while the live dependent hasn't settled");
  gate.resolve();
  await flow.idle();
  assert.deepEqual(events.slice(0, 3), ["pull 1", "inner first", "pull 2"]);
  gate = deferred();
  gate.resolve();
  await flow.idle();
  assert.equal(flow.get("inner").live, true);
  assert.equal(flow.get("src").version, 2);
  flow.reset();
});

test("pause() holds pulls; resume() continues them", async () => {
  const { flow, define } = sheet();
  let pulls = 0;
  define("src", [], async function* () {
    for (let i = 1; i <= 3; i++) {
      pulls++;
      yield i;
    }
  });
  flow.subscribe((id, s) => id === "src" && s.version === 1 && flow.pause());
  flow.run("src");
  await ticks(5);
  assert.equal(pulls, 1, "no pull while paused");
  assert.equal(flow.get("src").version, 1);
  flow.resume();
  await flow.idle();
  assert.equal(pulls, 3);
  assert.equal(flow.get("src").version, 3);
});

for (const [how, stopIt] of [
  ["a newer run of the node (supersede)", (flow) => flow.run("live")],
  ["an input change (restart)", (flow) => flow.invalidate("input")],
  ["reset()", (flow) => flow.reset()],
  ["delete(id)", (flow) => flow.delete("live")],
  ["an input that fails (the node waits)", (flow, s) => (s.defs.set("input", () => Promise.reject(new Error("x"))), flow.invalidate("input"))],
]) {
  test(`${how} aborts the live run's signal and calls return(): the producer's finally runs, late items and emits are dropped`, async () => {
    const s = sheet();
    const { flow, define } = s;
    const producers = [];
    const signals = [];
    const emits = [];
    define("input", [], () => 1);
    define("live", ["input"], (_input, ctx) => {
      const src = source(ctx.signal);
      producers.push(src);
      signals.push(ctx.signal);
      emits.push(ctx.emit);
      return src.iterable;
    });
    flow.runAll();
    await ticks(2);
    producers[0].push("first");
    await flow.idle();
    assert.equal(flow.get("live")?.version, 1);
    const version = flow.get("live").version;
    stopIt(flow, s);
    await ticks(5); // a restarted run is running with nothing yielded: idle() would wait for it
    assert.equal(signals[0].aborted, true, "signal aborted");
    assert.equal(producers[0].log.cleaned, true, "finally ran");
    assert.equal(producers[0].log.aborted, true, "the signal had fired by the time finally ran");
    assert.equal(emits[0]("late"), false, "a late emit is refused");
    producers[0].push("late item");
    await ticks(5);
    const state = flow.get("live");
    if (state) {
      assert.notEqual(state.result, "late");
      assert.notEqual(state.result, "late item");
      assert.ok(state.version <= version + (state.status === "done" ? 0 : 1));
    }
    flow.reset();
  });
}

test("a producer suspended at a yield (held by backpressure) runs its finally on return(), signal or not", async () => {
  const { flow, define } = sheet();
  const log = [];
  const slow = deferred();
  define("live", [], async function* () {
    try {
      for (let i = 1; ; i++) yield i; // never looks at the signal
    } finally {
      log.push("finally");
    }
  });
  define("dep", ["live"], () => slow.promise);
  flow.run("live");
  await ticks(3);
  assert.equal(flow.get("live").version, 1, "held at its yield while dep runs");
  flow.delete("live");
  await ticks(1);
  assert.deepEqual(log, ["finally"]);
  slow.resolve();
  await flow.idle();
  assert.equal(flow.get("dep").status, "waiting");
});

test("a producer awaiting something (not at a yield) still runs its finally when superseded, via the aborted signal", async () => {
  const { flow, define } = sheet();
  const log = [];
  define("live", [], async function* (ctx) {
    try {
      yield "first";
      await new Promise((_, reject) => ctx.signal.addEventListener("abort", () => reject(ctx.signal.reason)));
      yield "never";
    } finally {
      log.push(`finally ${ctx.runId}`);
    }
  });
  flow.run("live");
  await flow.idle();
  assert.equal(flow.get("live").result, "first");
  flow.run("live"); // supersede while the first run is awaiting
  await flow.idle();
  assert.deepEqual(log, ["finally 1"]);
  assert.equal(flow.get("live").runId, 2);
  assert.equal(flow.get("live").result, "first", "the second run's first item");
  flow.delete("live");
  await flow.idle();
  assert.deepEqual(log, ["finally 1", "finally 2"]);
});

test("an iterable returned by a run that was superseded before it resolved is closed unread", async () => {
  const { flow, define } = sheet();
  const gate = deferred();
  const closed = [];
  let call = 0;
  define("live", [], async () => {
    const n = ++call;
    if (n === 1) await gate.promise;
    let sent = false;
    return {
      [Symbol.asyncIterator]() {
        return {
          next: () => (sent ? new Promise(() => {}) : ((sent = true), Promise.resolve({ value: `item ${n}`, done: false }))),
          return: () => (closed.push(n), Promise.resolve({ done: true })),
        };
      },
    };
  });
  flow.run("live");
  await tick();
  flow.run("live");
  await ticks(3);
  gate.resolve();
  await ticks(3);
  assert.deepEqual(closed, [1], "run 1's iterable was closed without being read");
  assert.equal(flow.get("live").result, "item 2");
  flow.reset();
  assert.deepEqual(closed, [1, 2]);
});

test("ctx.emit settles while the run is current, with no backpressure; the run stays live until it resolves", async () => {
  const { flow, define, runs, states } = sheet();
  const done = deferred();
  let emit;
  define("push", [], async (ctx) => {
    emit = ctx.emit;
    ctx.emit("a");
    await done.promise;
    return "ignored: a run that emitted settles through its emits";
  });
  const seen = [];
  define("dep", ["push"], async (_v, ctx) => {
    await tick();
    if (!ctx.signal.aborted) seen.push(flow.get("push").result);
  });
  flow.run("push");
  await flow.idle();
  assert.equal(flow.get("push").status, "done");
  assert.equal(flow.get("push").live, true);
  assert.deepEqual(seen, ["a"]);
  // Three emits in a row: each settles (version +3), dependents coalesce to the latest.
  assert.equal(emit("b"), true);
  assert.equal(emit("c"), true);
  assert.equal(emit("d"), true);
  await flow.idle();
  assert.equal(flow.get("push").version, 4);
  assert.deepEqual(seen, ["a", "d"]);
  runs.length = 0;
  done.resolve();
  await flow.idle();
  assert.equal(flow.get("push").live, false);
  assert.equal(flow.get("push").result, "d", "the resolved value doesn't settle again");
  assert.equal(flow.get("push").version, 4);
  assert.deepEqual(runs, []);
  assert.equal(emit("e"), false, "ended: later emits are ignored");
  assert.equal(states.filter(([id]) => id === "push").at(-1)[1].live, false);
});

test("ctx.close() ends the live run: later emits are refused, an open iterable is closed", async () => {
  const { flow, define } = sheet();
  let ctxRef;
  define("push", [], (ctx) => {
    ctxRef = ctx;
    ctx.emit(1);
    return new Promise(() => {}); // never resolves on its own
  });
  flow.run("push");
  await flow.idle();
  assert.equal(flow.get("push").live, true);
  ctxRef.close();
  assert.equal(flow.get("push").live, false);
  assert.equal(ctxRef.emit(2), false);
  assert.equal(ctxRef.signal.aborted, false, "a close is an end, not a cancellation");
  assert.equal(flow.get("push").result, 1);
  await flow.idle();

  const src = source();
  const slow = deferred();
  define("iter", [], (ctx) => {
    ctxRef = ctx;
    return src.iterable;
  });
  define("dep", ["iter"], () => slow.promise); // holds the producer at its yield
  flow.run("iter");
  src.push("x", "y");
  await ticks(3);
  ctxRef.close();
  await ticks(1);
  assert.equal(src.log.cleaned, true);
  assert.equal(src.log.pulls, 1, "y was never pulled");
  slow.resolve();
  await flow.idle();
  assert.equal(flow.get("iter").live, false);
  assert.equal(flow.get("iter").result, "x");
});

test("a run can emit and then return an async iterable: both settle, in order", async () => {
  const { flow, define } = sheet();
  const results = [];
  let version = 0;
  flow.subscribe((id, s) => id === "both" && s.version !== version && (version = s.version) && results.push(s.result));
  define("both", [], (ctx) => {
    ctx.emit("cached");
    return (async function* () {
      yield "fresh 1";
      yield "fresh 2";
    })();
  });
  flow.run("both");
  await flow.idle();
  assert.deepEqual(results, ["cached", "fresh 1", "fresh 2"]);
  assert.equal(flow.get("both").version, 3);
  assert.equal(flow.get("both").live, false);
});

test("an emitting run that rejects settles error and ends", async () => {
  const { flow, define } = sheet();
  const fail = deferred();
  define("push", [], async (ctx) => {
    ctx.emit(1);
    await fail.promise;
  });
  flow.run("push");
  await flow.idle();
  fail.reject(new Error("lost connection"));
  await flow.idle();
  assert.equal(flow.get("push").status, "error");
  assert.equal(flow.get("push").live, false);
  assert.equal(flow.get("push").version, 2);
});

test("restart (the default): an input change supersedes a live run and starts a new one", async () => {
  const { flow, define, store } = sheet();
  const log = [];
  define("rate", [], store("rate", () => 2));
  define("ticker", ["rate"], async function* (rate, ctx) {
    try {
      yield `every ${rate}`;
      await new Promise((resolve) => ctx.signal.addEventListener("abort", resolve));
    } finally {
      log.push(`finally ${ctx.runId}`);
    }
  });
  flow.runAll();
  await flow.idle();
  assert.equal(flow.get("ticker").result, "every 2");
  flow.invalidate("rate");
  await flow.idle();
  assert.equal(flow.get("ticker").runId, 2);
  assert.deepEqual(log, ["finally 1"]);
  flow.reset();
});

test("resume: an input change doesn't supersede a live resume run; ctx.changes and ctx.onChange tell it", async () => {
  const { flow, define, store, values } = sheet();
  const log = [];
  const callbacks = [];
  let amount = 1;
  define("amount", [], store("amount", () => amount));
  define(
    "total",
    ["amount"],
    async function* (first, ctx) {
      ctx.onChange((notice) => callbacks.push(notice));
      try {
        let total = first;
        yield total;
        for await (const change of ctx.changes) {
          log.push(change.changed.join(","));
          total += values.get("amount");
          yield total;
        }
      } finally {
        log.push("finally");
      }
    },
    { resume: true },
  );
  assert.equal(flow.resumes("total"), true);
  assert.equal(flow.resumes("amount"), false);
  flow.runAll();
  await flow.idle();
  assert.equal(flow.get("total").result, 1);
  for (amount of [10, 100]) {
    flow.invalidate("amount");
    await flow.idle();
  }
  assert.equal(flow.get("total").result, 111, "accumulated across changes: the run was resumed, not restarted");
  assert.equal(flow.get("total").runId, 1);
  assert.equal(flow.get("total").version, 3);
  assert.deepEqual(log, ["amount", "amount"]);
  assert.equal(callbacks.length, 2);
  assert.deepEqual(callbacks[0], { deps: ["amount"], skipped: [], changed: ["amount"] });
  assert.ok(Object.isFrozen(callbacks[0]));
  // An explicit run restarts it (as does invalidate: a new definition).
  flow.run("total");
  await flow.idle();
  assert.equal(flow.get("total").runId, 2);
  assert.equal(flow.get("total").result, 100);
  assert.deepEqual(log, ["amount", "amount", "finally"]);
  flow.invalidate("total");
  await flow.idle();
  assert.equal(flow.get("total").runId, 3);
  flow.reset();
  await flow.idle();
  assert.equal(log.filter((l) => l === "finally").length, 3);
});

test("resume notices coalesce, latest wins, and are kept until the run takes them", async () => {
  const { flow, define, store } = sheet();
  const notices = [];
  const take = deferred();
  define("x", [], store("x", () => 1));
  define("y", [], store("y", () => 2));
  define(
    "r",
    ["x", "y"],
    async function* (_x, _y, ctx) {
      yield "ready";
      await take.promise; // busy elsewhere while changes arrive
      for await (const notice of ctx.changes) {
        notices.push(notice);
        yield notices.length;
      }
    },
    { resume: true },
  );
  flow.runAll();
  await flow.idle();
  flow.invalidate("x");
  await flow.idle();
  flow.invalidate("y");
  await flow.idle();
  flow.set("r", { deps: ["y", "x"], resume: true }); // the notice carries the deps as they are now
  flow.invalidate("x");
  await flow.idle();
  take.resolve();
  await flow.idle();
  assert.equal(notices.length, 1, "three changes, one notice");
  assert.deepEqual(notices[0], { deps: ["y", "x"], skipped: [], changed: ["x", "y"] });
  flow.invalidate("y");
  await flow.idle();
  assert.equal(notices.length, 2);
  assert.deepEqual(notices[1].changed, ["y"]);
  flow.reset();
});

test("resume notices are glitch-free: in a diamond, one notice per upstream change, after both branches settled", async () => {
  const { flow, define, store, values } = sheet();
  const seen = [];
  let a = 1;
  define("a", [], store("a", () => a));
  define("b", ["a"], async (v, ctx) => (await ticks(3), ctx.signal.aborted || values.set("b", v + 1), v + 1));
  define("c", ["a"], async (v, ctx) => (await ticks(1), ctx.signal.aborted || values.set("c", v * 10), v * 10));
  define(
    "join",
    ["b", "c"],
    (_b, _c, ctx) => {
      ctx.onChange(() => seen.push([values.get("b"), values.get("c")]));
      ctx.emit("ready");
      return new Promise(() => {});
    },
    { resume: true },
  );
  flow.runAll();
  await flow.idle();
  for (a of [2, 3]) {
    flow.invalidate("a");
    await flow.idle();
  }
  assert.deepEqual(seen, [
    [3, 20],
    [4, 30],
  ]);
  assert.equal(flow.get("join").runId, 1);
  flow.reset();
});

test("resume applies only to a live run: a resume node that isn't live (plain value, or not yet live) restarts as today", async () => {
  const { flow, define, store, runs } = sheet();
  let x = 1;
  define("x", [], store("x", () => x));
  define("plain", ["x"], store("plain", (v) => v * 2), { resume: true });
  flow.runAll();
  await flow.idle();
  runs.length = 0;
  x = 2;
  flow.invalidate("x");
  await flow.idle();
  assert.deepEqual(runs, ["x", "plain"]);
  assert.equal(flow.get("plain").runId, 2);
  assert.equal(flow.get("plain").result, 4);
});

test("a resume node with a skipped or failing input is skipped or waits, ending its live run", async () => {
  const { flow, define, defs } = sheet();
  const log = [];
  define("x", [], () => 1);
  define(
    "r",
    ["x"],
    async function* (_x, ctx) {
      try {
        yield "on";
        await new Promise((resolve) => ctx.signal.addEventListener("abort", resolve));
      } finally {
        log.push("finally");
      }
    },
    { resume: true },
  );
  flow.runAll();
  await flow.idle();
  defs.set("x", () => SKIP);
  flow.invalidate("x");
  await flow.idle();
  assert.equal(flow.get("r").status, "skipped");
  assert.equal(flow.get("r").live, false);
  assert.deepEqual(log, ["finally"]);
  defs.set("x", () => 2);
  flow.invalidate("x");
  await flow.idle();
  assert.equal(flow.get("r").live, true);
  defs.set("x", () => {
    throw new Error("x broke");
  });
  flow.invalidate("x");
  await flow.idle();
  assert.equal(flow.get("r").status, "waiting");
  assert.deepEqual(log, ["finally", "finally"]);
});

test("ctx.changes never yields for a node without resume, and ends when the run ends", async () => {
  const { flow, define } = sheet();
  const out = [];
  define("x", [], () => 1);
  define("n", ["x"], async (_x, ctx) => {
    ctx.emit("live");
    for await (const c of ctx.changes) out.push(c);
    out.push(`ended ${ctx.runId}`);
  });
  flow.runAll();
  await flow.idle();
  flow.invalidate("x"); // restarts n: run 1's changes end
  await flow.idle();
  assert.deepEqual(out, ["ended 1"], "run 1's loop ended when it was superseded; no notice was ever yielded");
  flow.reset();
  await flow.idle();
  assert.deepEqual(out, ["ended 1", "ended 2"]);
});

test("held runs: a held live node's settles flag dependents stale instead of running them, and nothing holds the producer", async () => {
  const { flow, define, store, runs } = sheet();
  let pulls = 0;
  let n = 0;
  define("src", [], async function* () {
    for (let i = 1; i <= 4; i++) {
      pulls++;
      yield `${++n}`;
    }
  });
  define("dep", ["src"], store("dep", (v) => v));
  flow.runAll();
  await flow.idle();
  runs.length = 0;
  flow.run("src", { follow: false });
  await flow.idle();
  assert.deepEqual(runs, ["src"], "the dependent never ran");
  assert.equal(pulls, 8);
  assert.equal(flow.get("dep").stale, true);
  assert.equal(flow.get("dep").status, "done");
  assert.equal(flow.get("src").version, 8);
  // An emitting held run holds too.
  define("emitter", [], (ctx) => {
    ctx.emit(1);
    ctx.emit(2);
    ctx.close();
  });
  define("dep2", ["emitter"], () => "d2");
  flow.runAll({ ids: ["emitter", "dep2"] });
  await flow.idle();
  runs.length = 0;
  flow.run("emitter", { follow: false });
  await flow.idle();
  assert.deepEqual(runs, ["emitter"]);
  assert.equal(flow.get("dep2").stale, true);
});

test("a graph with no live features: every state carries live: false, and ctx gains emit/close/changes/onChange", async () => {
  const states = [];
  const contexts = [];
  const flow = createDataflow({
    run(id, ctx) {
      contexts.push(ctx);
      return id;
    },
    onChange: (_id, s) => states.push(s),
  });
  flow.set("a").set("b", { deps: ["a"] });
  flow.runAll();
  await flow.idle();
  assert.deepEqual(
    states.map((s) => s.status),
    ["idle", "idle", "running", "done", "running", "done"],
  );
  assert.ok(states.every((s) => s.live === false));
  assert.ok(contexts.every((c) => typeof c.emit === "function" && typeof c.close === "function" && typeof c.onChange === "function" && typeof c.changes[Symbol.asyncIterator] === "function"));
  assert.equal(contexts[0].emit("late"), false, "after a plain run settles, emit is ignored");
  assert.equal(flow.get("a").result, "a");
  assert.equal(flow.resumes("missing"), undefined);
});
