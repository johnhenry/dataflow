import { test } from "node:test";
import assert from "node:assert/strict";
import { createDataflow, UNCHANGED, unchanged, isUnchanged, isSkip } from "../src/index.mjs";

const delay = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * The caller's equality cut-off: values live outside the scheduler, and `run`
 * returns UNCHANGED when the new value equals the stored one.
 */
function sheet({ withResult = false } = {}) {
  const values = new Map();
  const defs = new Map();
  const runs = [];
  const flow = createDataflow({
    async run(id, { deps, signal }) {
      runs.push(id);
      const value = await defs.get(id)(...deps.map((d) => values.get(d)));
      if (signal.aborted) return value;
      if (values.has(id) && Object.is(values.get(id), value)) return withResult ? unchanged(value) : UNCHANGED;
      values.set(id, value);
      return value;
    },
  });
  const define = (id, deps, fn, opts = {}) => {
    defs.set(id, fn);
    flow.set(id, { deps, ...opts });
  };
  return { flow, values, defs, runs, define };
}

test("UNCHANGED, unchanged(result) and isUnchanged", () => {
  assert.equal(UNCHANGED, Symbol.for("@johnhenry/dataflow.unchanged"));
  assert.ok(isUnchanged(UNCHANGED));
  assert.ok(isUnchanged(unchanged(3)));
  assert.equal(unchanged(3).result, 3);
  assert.ok(Object.isFrozen(unchanged(3)));
  assert.ok(!isUnchanged(undefined));
  assert.ok(!isUnchanged({ result: 3 }));
  assert.ok(!isSkip(UNCHANGED));
});

test("x => x % 2 going from 2 to 4 doesn't rerun dependents (equality cut-off)", async () => {
  const { flow, defs, runs, define, values } = sheet();
  define("x", [], () => 2);
  define("parity", ["x"], (x) => x % 2);
  define("label", ["parity"], (p) => (p ? "odd" : "even"));
  flow.runAll();
  await flow.idle();
  const before = flow.get("parity");
  runs.length = 0;
  defs.set("x", () => 4);
  flow.invalidate("x");
  await flow.idle();
  assert.deepEqual(runs, ["x", "parity"], "label didn't rerun");
  const after = flow.get("parity");
  assert.equal(after.status, "done");
  assert.equal(after.unchanged, true);
  assert.equal(after.version, before.version, "an unchanged run doesn't bump version");
  assert.equal(after.result, 0, "the previous result is kept");
  assert.equal(after.runId, before.runId + 1, "it was still a run");
  assert.equal(flow.get("x").unchanged, false);
  assert.equal(values.get("label"), "even");
  // A real change propagates again, and clears the flag.
  defs.set("x", () => 5);
  flow.invalidate("x");
  await flow.idle();
  assert.deepEqual(runs, ["x", "parity", "x", "parity", "label"]);
  assert.equal(flow.get("parity").unchanged, false);
  assert.equal(values.get("label"), "odd");
});

test("unchanged(result) replaces state.result", async () => {
  const { flow, defs, define, runs } = sheet({ withResult: true });
  define("a", [], () => "same");
  define("b", ["a"], (a) => a.length);
  flow.runAll();
  await flow.idle();
  runs.length = 0;
  flow.invalidate("a");
  await flow.idle();
  assert.deepEqual(runs, ["a"]);
  assert.equal(flow.get("a").result, "same");
  assert.equal(flow.get("a").unchanged, true);
  void defs;
});

test("manual dependents aren't flagged stale by an unchanged run", async () => {
  const { flow, define, runs } = sheet();
  define("a", [], () => 1);
  define("b", ["a"], (a) => a + 1, { autorun: false });
  flow.runAll();
  await flow.idle();
  runs.length = 0;
  flow.run("a");
  await flow.idle();
  assert.deepEqual(runs, ["a"]);
  assert.equal(flow.get("b").stale, false);
});

test("UNCHANGED on a first run (nothing to compare with) counts as a new value", async () => {
  const runs = [];
  const flow = createDataflow({
    run(id) {
      runs.push(id);
      return id === "a" ? UNCHANGED : "b ran";
    },
  });
  flow.set("a").set("b", { deps: ["a"] });
  flow.runAll({ ids: ["a"] });
  await flow.idle();
  assert.deepEqual(runs, ["a", "b"]);
  assert.equal(flow.get("a").status, "done");
  assert.equal(flow.get("a").unchanged, false);
  assert.equal(flow.get("a").version, 1);
  assert.equal(flow.get("a").result, undefined);
});

test("UNCHANGED after an error counts as a new value: waiting dependents run", async () => {
  let mode = "ok";
  const runs = [];
  const flow = createDataflow({
    run(id) {
      runs.push(id);
      if (id === "a") {
        if (mode === "fail") throw new Error("boom");
        return mode === "same" ? UNCHANGED : 1;
      }
      return "b";
    },
  });
  flow.set("a").set("b", { deps: ["a"] });
  flow.runAll();
  await flow.idle();
  mode = "fail";
  flow.run("a");
  await flow.idle();
  assert.equal(flow.get("b").status, "waiting");
  mode = "same";
  runs.length = 0;
  flow.run("a");
  await flow.idle();
  assert.deepEqual(runs, ["a", "b"]);
  assert.equal(flow.get("b").status, "done");
});

test("an unchanged run still queues waiting and never-run autorun dependents", async () => {
  const { flow, define, runs } = sheet();
  define("a", [], () => 1);
  flow.runAll();
  await flow.idle();
  define("fresh", ["a"], (a) => a + 1); // added after a ran; never run
  define("manual", ["a"], (a) => a + 2, { autorun: false }); // never run, manual: stays idle
  runs.length = 0;
  flow.run("a");
  await flow.idle();
  assert.deepEqual(runs, ["a", "fresh"]);
  assert.equal(flow.get("manual").status, "idle");
});

test("a dependent queued for its own reasons still runs after an unchanged input", async () => {
  const { flow, define, runs } = sheet();
  define("a", [], async () => (await delay(2), 1));
  define("b", ["a"], (a) => a + 1);
  flow.runAll();
  await flow.idle();
  runs.length = 0;
  flow.runAll({ ids: ["a", "b"] });
  await flow.idle();
  assert.deepEqual(runs, ["a", "b"]);
});

test("a superseded unchanged result is dropped like any other", async () => {
  let call = 0;
  const flow = createDataflow({
    async run() {
      call += 1;
      if (call === 1) return 1;
      if (call === 2) return (await delay(10), UNCHANGED);
      return 2;
    },
  });
  flow.set("a");
  flow.run("a");
  await flow.idle();
  flow.run("a");
  await delay(1);
  flow.run("a");
  await flow.idle();
  await delay(15);
  assert.equal(flow.get("a").result, 2);
  assert.equal(flow.get("a").unchanged, false);
  assert.equal(flow.get("a").version, 2);
});

test("reset() forgets the last value: UNCHANGED after a reset counts as new", async () => {
  const runs = [];
  let first = true;
  const flow = createDataflow({
    run(id) {
      runs.push(id);
      if (id === "a") {
        const out = first ? 1 : UNCHANGED;
        first = false;
        return out;
      }
      return "b";
    },
  });
  flow.set("a").set("b", { deps: ["a"] });
  flow.runAll();
  await flow.idle();
  flow.reset();
  runs.length = 0;
  flow.run("a");
  await flow.idle();
  assert.deepEqual(runs, ["a", "b"]);
});
