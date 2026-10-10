import { test } from "node:test";
import assert from "node:assert/strict";
import { createDataflow, SKIP } from "../src/index.mjs";

const delay = (ms) => new Promise((r) => setTimeout(r, ms));

/** Values live outside the scheduler; `defs` maps id -> (inputs...) => value. */
function sheet() {
  const values = new Map();
  const defs = new Map();
  const runs = [];
  const flow = createDataflow({
    async run(id, { deps, signal }) {
      runs.push(id);
      const value = await defs.get(id)(...deps.map((d) => values.get(d)));
      if (!signal.aborted) values.set(id, value);
      return value;
    },
  });
  const define = (id, deps, fn, opts = {}) => {
    defs.set(id, fn);
    flow.set(id, { deps, ...opts });
  };
  const stale = (...ids) => ids.map((id) => flow.get(id).stale);
  return { flow, values, defs, runs, define, stale };
}

/** a -> b -> c -> d, all autorun, all run once. */
async function chain() {
  const s = sheet();
  s.define("a", [], () => 1);
  s.define("b", ["a"], (a) => a + 1);
  s.define("c", ["b"], (b) => b * 10);
  s.define("d", ["c"], (c) => c + 5);
  s.flow.runAll();
  await s.flow.idle();
  s.runs.length = 0;
  return s;
}

test("run(id, { follow: false }) runs only that node; its autorun dependents are flagged stale", async () => {
  const { flow, runs, defs, values, stale } = await chain();
  defs.set("b", (a) => a + 100);
  flow.run("b", { follow: false });
  await flow.idle();
  assert.deepEqual(runs, ["b"]);
  assert.equal(values.get("b"), 101);
  assert.deepEqual(stale("b", "c", "d"), [false, true, false], "direct dependents are stale, as manual nodes would be");
  assert.equal(flow.get("c").status, "done", "a held dependent keeps its result");
  assert.equal(flow.get("c").result, 20);
  // Running the held dependent later clears the flag, and autorun follows from there.
  flow.run("c");
  await flow.idle();
  assert.deepEqual(runs, ["b", "c", "d"]);
  assert.deepEqual(stale("c", "d"), [false, false]);
  assert.equal(values.get("d"), 1015);
});

test("run(id) still lets autorun dependents follow (follow defaults to true)", async () => {
  const { flow, runs } = await chain();
  flow.run("b");
  await flow.idle();
  assert.deepEqual(runs, ["b", "c", "d"]);
  flow.run("b", { follow: true });
  await flow.idle();
  assert.deepEqual(runs, ["b", "c", "d", "b", "c", "d"]);
});

test("runAll({ ids, follow: false }) runs exactly the set, in order, and holds everything outside it", async () => {
  const { flow, runs, stale } = await chain();
  flow.runAll({ ids: ["c", "a", "b"], follow: false });
  await flow.idle();
  assert.deepEqual(runs, ["a", "b", "c"], "dependency order, each once");
  assert.deepEqual(stale("a", "b", "c", "d"), [false, false, false, true]);
});

test("a held diamond runs each queued node once and holds the join", async () => {
  const { flow, define, runs, stale } = sheet();
  define("a", [], () => 1);
  define("b", ["a"], async (a) => (await delay(5), a + 1));
  define("c", ["a"], async (a) => (await delay(1), a * 10));
  define("d", ["b", "c"], (b, c) => b + c);
  flow.runAll();
  await flow.idle();
  runs.length = 0;
  flow.runAll({ ids: ["a", "b", "c"], follow: false });
  await flow.idle();
  assert.deepEqual(runs.sort(), ["a", "b", "c"]);
  assert.deepEqual(stale("d"), [true]);
});

test("runAll({ ids }) with follow (the default) runs manual nodes in the set: a one-shot 'run from here'", async () => {
  const { flow, define, runs } = sheet();
  define("a", [], () => 1);
  define("b", ["a"], (a) => a + 1);
  define("c", ["b"], (b) => b + 1, { autorun: false });
  flow.runAll();
  await flow.idle();
  runs.length = 0;
  flow.runAll({ ids: ["b", "c"] });
  await flow.idle();
  assert.deepEqual(runs, ["b", "c"]);
  assert.equal(flow.get("c").stale, false);
});

test("a held run that fails still makes dependents wait; waiting dependents still run when it recovers, as manual ones do", async () => {
  const { flow, defs, runs, values } = await chain();
  defs.set("a", () => {
    throw new Error("broken");
  });
  flow.run("a", { follow: false });
  await flow.idle();
  assert.deepEqual(["b", "c", "d"].map((id) => flow.get(id).status), ["waiting", "waiting", "waiting"]);
  runs.length = 0;
  defs.set("a", () => 7);
  flow.run("a", { follow: false });
  await flow.idle();
  // Waiting means "already asked to run": a hold doesn't cancel that (same as autorun: false).
  assert.deepEqual(runs, ["a", "b", "c", "d"]);
  assert.equal(values.get("d"), 85);
});

test("a held request on a node whose input is broken survives the wait", async () => {
  const { flow, define, defs, runs } = sheet();
  define("a", [], () => 1);
  define("b", ["a"], (a) => a + 1, { autorun: false });
  define("c", ["b"], (b) => b * 10);
  flow.runAll();
  await flow.idle();
  define("d", ["c"], (c) => c + 5); // added later, never run: stays idle
  defs.set("a", () => {
    throw new Error("broken");
  });
  flow.run("a");
  await flow.idle();
  flow.run("c", { follow: false });
  await flow.idle();
  assert.equal(flow.get("c").status, "waiting");
  runs.length = 0;
  defs.set("a", () => 2);
  flow.run("a");
  await flow.idle();
  // b (manual, waiting) runs; c runs with its held request, so d doesn't follow.
  assert.deepEqual(runs, ["a", "b", "c"]);
  assert.equal(flow.get("d").status, "idle");
});

test("a held run that skips flags its dependents stale instead of spreading the skip", async () => {
  const { flow, defs, runs, stale } = await chain();
  defs.set("b", () => SKIP);
  flow.run("b", { follow: false });
  await flow.idle();
  assert.equal(flow.get("b").status, "skipped");
  assert.equal(flow.get("c").status, "done");
  assert.deepEqual(stale("c"), [true]);
  assert.deepEqual(runs, ["b"]);
});

test("a later plain run() of a queued node replaces its held request (the latest request wins)", async () => {
  const { flow, runs } = await chain();
  flow.run("b", { follow: false });
  flow.run("b");
  await flow.idle();
  assert.deepEqual(runs, ["b", "c", "d"]);
});

test("reset() forgets a pending hold", async () => {
  const { flow, runs } = await chain();
  flow.pause();
  flow.run("b", { follow: false });
  flow.reset();
  flow.resume();
  flow.runAll();
  await flow.idle();
  assert.deepEqual(runs, ["a", "b", "c", "d"]);
  assert.equal(flow.get("d").stale, false);
});
