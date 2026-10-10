import { test } from "node:test";
import assert from "node:assert/strict";
import { createDataflow } from "../src/index.mjs";

const delay = (ms) => new Promise((r) => setTimeout(r, ms));

/** A tiny "spreadsheet": values live outside the scheduler, like they would in a Worker. */
function sheet() {
  const values = new Map();
  const defs = new Map();
  const runs = [];
  const flow = createDataflow({
    async run(id, { deps, signal }) {
      runs.push(id);
      const def = defs.get(id);
      const value = await def(...deps.map((d) => values.get(d)), { signal });
      if (!signal.aborted) values.set(id, value);
      return value;
    },
  });
  const define = (id, deps, fn, opts = {}) => {
    defs.set(id, fn);
    flow.set(id, { deps, ...opts });
  };
  return { flow, values, defs, runs, define };
}

test("a run propagates to autorun dependents in order", async () => {
  const { flow, values, define, runs } = sheet();
  define("price", [], () => 50);
  define("tip", ["price"], (p) => p * 0.2);
  define("total", ["price", "tip"], (p, t) => p + t);
  flow.runAll();
  await flow.idle();
  assert.equal(values.get("total"), 60);
  assert.deepEqual(runs, ["price", "tip", "total"]);
  assert.equal(flow.get("total").status, "done");
  assert.equal(flow.get("total").result, 60);
});

test("a diamond's join runs once per upstream change (no glitch)", async () => {
  const { flow, define, runs, values } = sheet();
  define("a", [], () => 1);
  define("b", ["a"], async (a) => (await delay(5), a + 1));
  define("c", ["a"], async (a) => (await delay(1), a * 10));
  define("d", ["b", "c"], (b, c) => b + c);
  flow.runAll();
  await flow.idle();
  runs.length = 0;
  flow.run("a");
  await flow.idle();
  assert.deepEqual(runs.filter((r) => r === "d"), ["d"]);
  assert.equal(values.get("d"), 12);
});

test("a newer run supersedes an older one: the old result is dropped and its signal aborts", async () => {
  let call = 0;
  const signals = [];
  const flow = createDataflow({
    async run(_id, { signal }) {
      const n = ++call;
      signals.push(signal);
      await delay(n === 1 ? 30 : 1);
      return n;
    },
  });
  flow.set("x");
  flow.run("x");
  await delay(5);
  flow.run("x");
  await flow.idle();
  await delay(40);
  assert.equal(flow.get("x").result, 2, "the slow first run must not overwrite the second");
  assert.equal(signals[0].aborted, true);
  assert.equal(signals[1].aborted, false);
});

test("an error does not cascade as an error: dependents wait, and recover when fixed", async () => {
  const { flow, define, defs, values } = sheet();
  define("a", [], () => 1);
  define("b", ["a"], (a) => a + 1);
  flow.runAll();
  await flow.idle();
  defs.set("a", () => {
    throw new Error("boom");
  });
  flow.invalidate("a");
  await flow.idle();
  assert.equal(flow.get("a").status, "error");
  assert.equal(flow.get("a").error.message, "boom");
  assert.equal(flow.get("b").status, "waiting");
  defs.set("a", () => 41);
  flow.invalidate("a");
  await flow.idle();
  assert.equal(flow.get("b").status, "done");
  assert.equal(values.get("b"), 42);
});

test("a queued, never-run dependent of a failing node waits, then runs once the failure is fixed", async () => {
  // Regression: markWaiting left queued-but-never-run nodes "idle", so they
  // never ran after the upstream error was fixed (found by examples/03).
  let broken = true;
  const flow = createDataflow({
    run(id) {
      if (id === "a" && broken) throw new Error("down");
      return id;
    },
  });
  flow.set("a").set("b", { deps: ["a"] }).set("c", { deps: ["b"] });
  flow.runAll();
  await flow.idle();
  assert.deepEqual(["a", "b", "c"].map((id) => flow.get(id).status), ["error", "waiting", "waiting"]);
  broken = false;
  flow.invalidate("a");
  await flow.idle();
  assert.deepEqual(["a", "b", "c"].map((id) => flow.get(id).status), ["done", "done", "done"]);
});

test("manual nodes keep their result and are marked stale until run", async () => {
  const { flow, define, values } = sheet();
  define("a", [], () => 1);
  define("b", ["a"], (a) => a * 100, { autorun: false });
  flow.runAll();
  await flow.idle();
  assert.equal(values.get("b"), 100);
  define("a", [], () => 2);
  flow.invalidate("a");
  await flow.idle();
  assert.equal(values.get("b"), 100, "manual node did not rerun");
  assert.equal(flow.get("b").stale, true);
  flow.run("b");
  await flow.idle();
  assert.equal(values.get("b"), 200);
  assert.equal(flow.get("b").stale, false);
});

test("invalidating a manual node marks it stale instead of running it", async () => {
  const { flow, define, runs } = sheet();
  define("m", [], () => 1, { autorun: false });
  flow.run("m");
  await flow.idle();
  runs.length = 0;
  flow.invalidate("m");
  await flow.idle();
  assert.deepEqual(runs, []);
  assert.equal(flow.get("m").stale, true);
});

test("a node asked to run before its input exists waits, then runs when the input arrives", async () => {
  const { flow, define, values } = sheet();
  define("a", [], () => 5);
  define("b", ["a"], (a) => a + 1, { autorun: false });
  flow.run("b");
  await flow.idle();
  assert.equal(flow.get("b").status, "waiting");
  flow.run("a");
  await flow.idle();
  assert.equal(flow.get("b").status, "done");
  assert.equal(values.get("b"), 6);
});

test("cycles are detected, refused, and released when broken", async () => {
  const { flow, define } = sheet();
  define("a", ["b"], (b) => b);
  define("b", ["a"], (a) => a);
  assert.equal(flow.get("a").status, "error");
  assert.equal(flow.get("a").cycle, true);
  assert.equal(flow.get("b").cycle, true);
  assert.deepEqual(flow.order(), []);
  define("b", [], () => 3);
  assert.equal(flow.get("a").cycle, false);
  assert.equal(flow.get("b").cycle, false);
  flow.runAll();
  await flow.idle();
  assert.equal(flow.get("a").result, 3);
});

test("a self-dependency is a cycle", () => {
  const { flow, define } = sheet();
  define("a", ["a"], (a) => a);
  assert.equal(flow.get("a").cycle, true);
});

test("deleting an input makes dependents wait", async () => {
  const { flow, define } = sheet();
  define("a", [], () => 1);
  define("b", ["a"], (a) => a);
  flow.runAll();
  await flow.idle();
  flow.delete("a");
  assert.equal(flow.has("a"), false);
  assert.equal(flow.get("b").status, "waiting");
});

test("reset forgets results and drops in-flight runs", async () => {
  const flow = createDataflow({ run: async () => (await delay(10), "late") });
  flow.set("x");
  flow.run("x");
  await delay(1);
  flow.reset();
  await delay(20);
  assert.equal(flow.get("x").status, "idle");
  assert.equal(flow.get("x").result, undefined);
});

test("pause holds runs; resume flushes them", async () => {
  const { flow, define, runs } = sheet();
  define("a", [], () => 1);
  flow.pause();
  flow.run("a");
  await delay(5);
  assert.deepEqual(runs, []);
  flow.resume();
  await flow.idle();
  assert.deepEqual(runs, ["a"]);
});

test("subscribe sees every transition, and unsubscribe stops it", async () => {
  const flow = createDataflow({ run: () => 1 });
  const seen = [];
  const off = flow.subscribe((id, s) => seen.push(`${id}:${s.status}`));
  flow.set("a");
  flow.run("a");
  await flow.idle();
  off();
  flow.run("a");
  await flow.idle();
  assert.deepEqual(seen, ["a:idle", "a:running", "a:done"]);
});

test("runAll({ all: false }) skips manual nodes", async () => {
  const { flow, define, runs } = sheet();
  define("a", [], () => 1);
  define("m", [], () => 2, { autorun: false });
  flow.runAll({ all: false });
  await flow.idle();
  assert.deepEqual(runs, ["a"]);
});

test("a synchronous throw from run is reported as an error state", async () => {
  const flow = createDataflow({
    run() {
      throw new TypeError("sync");
    },
  });
  flow.set("a").run("a");
  await flow.idle();
  assert.equal(flow.get("a").status, "error");
  assert.equal(flow.get("a").error.message, "sync");
});

test("set validates ids and createDataflow requires run", () => {
  assert.throws(() => createDataflow({}), TypeError);
  const flow = createDataflow({ run: () => 1 });
  assert.throws(() => flow.set(""), TypeError);
});

test("a throwing listener is reported but doesn't stall scheduling or other listeners", async () => {
  // Regression (found while writing the docs): an exception from onChange during
  // settle became an unhandled rejection, dependents never started, idle() hung.
  const reported = [];
  const original = globalThis.reportError;
  globalThis.reportError = (e) => reported.push(e.message);
  try {
    const seen = [];
    const flow = createDataflow({
      run: (id) => id,
      onChange() {
        throw new Error("listener bug");
      },
    });
    flow.subscribe((id, s) => seen.push(`${id}:${s.status}`));
    flow.set("a").set("b", { deps: ["a"] });
    flow.runAll();
    await flow.idle();
    assert.equal(flow.get("b").status, "done");
    assert.ok(seen.includes("b:done"), "other listeners still ran");
    assert.ok(reported.length > 0 && reported.every((m) => m === "listener bug"));
  } finally {
    globalThis.reportError = original;
  }
});
