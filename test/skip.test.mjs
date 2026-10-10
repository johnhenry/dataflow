import { test } from "node:test";
import assert from "node:assert/strict";
import { createDataflow, SKIP, skip, isSkip } from "../src/index.mjs";

const delay = (ms) => new Promise((r) => setTimeout(r, ms));

/** Values live outside the scheduler; `defs` maps id -> (inputs..., ctx) => value | SKIP. */
function sheet() {
  const values = new Map();
  const defs = new Map();
  const runs = [];
  const contexts = new Map();
  const flow = createDataflow({
    async run(id, ctx) {
      runs.push(id);
      contexts.set(id, ctx);
      const value = await defs.get(id)(...ctx.deps.map((d) => values.get(d)), ctx);
      if (!ctx.signal.aborted) {
        if (isSkip(value)) values.delete(id); // a skipped node has no value
        else values.set(id, value);
      }
      return value;
    },
  });
  const define = (id, deps, fn, opts = {}) => {
    defs.set(id, fn);
    flow.set(id, { deps, ...opts });
  };
  const statuses = (...ids) => ids.map((id) => flow.get(id).status);
  return { flow, values, defs, runs, contexts, define, statuses };
}

/** Coalesce: the first input that wasn't skipped, or a skip when they all were. */
const coalesce = (...args) => {
  const { deps, skipped } = args.at(-1);
  const i = deps.findIndex((d) => !skipped.includes(d));
  return i === -1 ? SKIP : args[i];
};

test("SKIP, skip(reason) and isSkip", () => {
  assert.equal(typeof SKIP, "symbol");
  assert.equal(SKIP, Symbol.for("@johnhenry/dataflow.skip"), "two copies of the package agree");
  assert.equal(isSkip(SKIP), true);
  const s = skip("no match");
  assert.equal(isSkip(s), true);
  assert.equal(s.reason, "no match");
  assert.ok(Object.isFrozen(s));
  for (const v of [undefined, null, 0, "", false, {}, [], "skip", Symbol("skip")]) assert.equal(isSkip(v), false);
});

test("a node that returns SKIP settles as skipped", async () => {
  const { flow, define } = sheet();
  define("a", [], () => SKIP);
  flow.run("a");
  await flow.idle();
  const s = flow.get("a");
  assert.equal(s.status, "skipped");
  assert.deepEqual(s.skip, { source: "a", reason: undefined });
  assert.equal(s.result, undefined);
  assert.equal(s.version, 1, "a run settled");
});

test("a synchronous SKIP and a promise of skip(reason) both work", async () => {
  const flow = createDataflow({ run: (id) => (id === "sync" ? SKIP : Promise.resolve(skip("later"))) });
  flow.set("sync").set("async").runAll();
  await flow.idle();
  assert.equal(flow.get("sync").status, "skipped");
  assert.equal(flow.get("async").status, "skipped");
  assert.equal(flow.get("async").skip.reason, "later");
});

test("a skip spreads down a chain without running the dependents", async () => {
  const { flow, define, runs, statuses } = sheet();
  define("a", [], () => skip("nothing to do"));
  define("b", ["a"], (a) => a + 1);
  define("c", ["b"], (b) => b * 2);
  flow.runAll();
  await flow.idle();
  assert.deepEqual(runs, ["a"]);
  assert.deepEqual(statuses("a", "b", "c"), ["skipped", "skipped", "skipped"]);
  assert.deepEqual(flow.get("c").skip, { source: "a", reason: "nothing to do" }, "source and reason travel with the skip");
  assert.equal(flow.get("b").version, 0, "spreading doesn't count as a run");
  assert.equal(flow.get("c").version, 0);
});

test("in a diamond, one skipped branch skips the join, once, without running it", async () => {
  const { flow, define, runs } = sheet();
  const seen = [];
  flow.subscribe((id, s) => id === "d" && seen.push(s.status));
  define("a", [], () => 1);
  define("b", ["a"], async () => (await delay(5), SKIP));
  define("c", ["a"], async (a) => (await delay(1), a * 10));
  define("d", ["b", "c"], (b, c) => b + c);
  flow.runAll();
  await flow.idle();
  assert.equal(flow.get("d").status, "skipped");
  assert.equal(flow.get("d").skip.source, "b");
  assert.ok(!runs.includes("d"));
  assert.deepEqual(seen, ["idle", "skipped"], "the join is decided once, after both branches settled");
});

test("a consumer runs with skipped inputs and sees which ones (coalesce)", async () => {
  const { flow, define, values, contexts } = sheet();
  define("a", [], () => 1);
  define("b", ["a"], () => SKIP);
  define("c", ["a"], (a) => a + 41);
  define("d", ["b", "c"], coalesce, { consumesSkip: true });
  flow.runAll();
  await flow.idle();
  assert.equal(flow.get("d").status, "done");
  assert.equal(values.get("d"), 42);
  assert.deepEqual(contexts.get("d").skipped, ["b"]);
  assert.equal(flow.consumesSkip("d"), true);
});

test("a coalesce whose inputs were all skipped can skip itself, and that spreads on", async () => {
  const { flow, define, runs, statuses } = sheet();
  define("b", [], () => skip("b off"));
  define("c", [], () => skip("c off"));
  define("d", ["b", "c"], coalesce, { consumesSkip: true });
  define("e", ["d"], (d) => d);
  flow.runAll();
  await flow.idle();
  assert.ok(runs.includes("d"), "the consumer ran");
  assert.ok(!runs.includes("e"));
  assert.deepEqual(statuses("d", "e"), ["skipped", "skipped"]);
  assert.deepEqual(flow.get("e").skip, { source: "d", reason: undefined }, "the source is the node that returned the skip");
});

test("consumesSkip can name the inputs it handles; a skip on any other input still spreads", async () => {
  const { flow, define, runs, contexts, defs } = sheet();
  define("optional", [], () => SKIP);
  define("required", [], () => 2);
  define("out", ["optional", "required"], (o, r, ctx) => (ctx.skipped.includes("optional") ? r : o + r), { consumesSkip: ["optional"] });
  flow.runAll();
  await flow.idle();
  assert.equal(flow.get("out").status, "done");
  assert.equal(flow.get("out").result, 2);
  assert.deepEqual(contexts.get("out").skipped, ["optional"]);
  assert.deepEqual(flow.consumesSkip("out"), ["optional"]);

  runs.length = 0;
  defs.set("required", () => skip("gone"));
  flow.invalidate("required");
  await flow.idle();
  assert.deepEqual(runs, ["required"], "out didn't run");
  assert.equal(flow.get("out").status, "skipped");
  assert.equal(flow.get("out").skip.reason, "gone");
});

test("if/else: mutually exclusive branches, and a consumer that takes whichever ran", async () => {
  const { flow, define, values, runs } = sheet();
  let cond = true;
  define("cond", [], () => cond);
  define("then", ["cond"], (c) => (c ? "yes" : SKIP));
  define("else", ["cond"], (c) => (c ? SKIP : "no"));
  define("then.len", ["then"], (t) => t.length);
  define("out", ["then", "else"], coalesce, { consumesSkip: true });
  flow.runAll();
  await flow.idle();
  assert.equal(values.get("out"), "yes");
  assert.equal(flow.get("then.len").status, "done");
  assert.equal(flow.get("else").status, "skipped");

  cond = false;
  runs.length = 0;
  flow.invalidate("cond");
  await flow.idle();
  assert.equal(values.get("out"), "no");
  assert.equal(flow.get("then").status, "skipped");
  assert.equal(flow.get("then.len").status, "skipped");
  assert.ok(!runs.includes("then.len"));
  assert.equal(runs.filter((r) => r === "out").length, 1, "the join ran once");
});

test("a skipped chain recovers when its source produces a value again", async () => {
  const { flow, define, defs, values, statuses } = sheet();
  define("a", [], () => SKIP);
  define("b", ["a"], (a) => a + 1);
  define("c", ["b"], (b) => b * 2);
  flow.runAll();
  await flow.idle();
  assert.deepEqual(statuses("a", "b", "c"), ["skipped", "skipped", "skipped"]);
  defs.set("a", () => 4);
  flow.invalidate("a");
  await flow.idle();
  assert.deepEqual(statuses("a", "b", "c"), ["done", "done", "done"]);
  assert.equal(values.get("c"), 10);
  for (const id of ["a", "b", "c"]) assert.equal("skip" in flow.get(id), false, "no leftover skip info");
  // ...and goes back to skipped, dropping the result.
  defs.set("a", () => SKIP);
  flow.invalidate("a");
  await flow.idle();
  assert.deepEqual(statuses("a", "b", "c"), ["skipped", "skipped", "skipped"]);
  assert.equal(flow.get("c").result, undefined);
});

test("newest run wins: a superseded SKIP is dropped, and so is a superseded value", async () => {
  let call = 0;
  const flow = createDataflow({
    async run() {
      const n = ++call;
      await delay(n === 1 ? 30 : 1);
      return n === 1 ? SKIP : "second";
    },
  });
  flow.set("x").set("y", { deps: ["x"] });
  flow.run("x");
  await delay(5);
  flow.run("x");
  await flow.idle();
  await delay(40);
  assert.equal(flow.get("x").status, "done");
  assert.equal(flow.get("x").result, "second");

  let call2 = 0;
  const flow2 = createDataflow({
    async run() {
      const n = ++call2;
      await delay(n === 1 ? 30 : 1);
      return n === 1 ? "first" : SKIP;
    },
  });
  flow2.set("x");
  flow2.run("x");
  await delay(5);
  flow2.run("x");
  await flow2.idle();
  await delay(40);
  assert.equal(flow2.get("x").status, "skipped");
  assert.equal(flow2.get("x").result, undefined);
});

test("a skip spreading onto a node aborts that node's in-flight run and drops its result", async () => {
  let aValue = 1;
  const signals = [];
  const flow = createDataflow({
    async run(id, { signal }) {
      if (id === "a") return aValue;
      signals.push(signal);
      await delay(30);
      return "late b";
    },
  });
  flow.set("a").set("b", { deps: ["a"] }).set("c", { deps: ["b"] });
  flow.runAll();
  await delay(5); // b is running
  assert.equal(flow.get("b").status, "running");
  const runningId = flow.get("b").runId;
  aValue = SKIP;
  flow.invalidate("a");
  await flow.idle();
  await delay(40);
  assert.equal(signals[0].aborted, true);
  assert.equal(flow.get("b").status, "skipped");
  assert.equal(flow.get("b").result, undefined, "the late result was dropped");
  assert.ok(flow.get("b").runId > runningId, "the superseded run's id was bumped");
  assert.equal(flow.get("c").status, "skipped");
});

test("errors still make dependents wait, also consumers, and an error beats a skip", async () => {
  const { flow, define, defs, statuses, runs } = sheet();
  define("bad", [], () => {
    throw new Error("boom");
  });
  define("off", [], () => SKIP);
  define("join", ["bad", "off"], (x) => x);
  define("co", ["bad", "off"], coalesce, { consumesSkip: true });
  flow.runAll();
  await flow.idle();
  assert.deepEqual(statuses("bad", "off", "join", "co"), ["error", "skipped", "waiting", "waiting"]);
  assert.ok(!runs.includes("co"));
  // Fix the error with a skip: the waiting nodes resolve to skipped / run.
  defs.set("bad", () => SKIP);
  flow.invalidate("bad");
  await flow.idle();
  assert.deepEqual(statuses("join", "co"), ["skipped", "skipped"]);
  assert.ok(runs.includes("co"));
});

test("an error upstream of a skipped node makes it wait; a skip upstream of a waiting node skips it", async () => {
  const { flow, define, defs, statuses } = sheet();
  define("a", [], () => SKIP);
  define("b", ["a"], (a) => a);
  define("c", ["b"], (b) => b);
  flow.runAll();
  await flow.idle();
  defs.set("a", () => {
    throw new Error("down");
  });
  flow.invalidate("a");
  await flow.idle();
  assert.deepEqual(statuses("a", "b", "c"), ["error", "waiting", "waiting"]);
  assert.equal("skip" in flow.get("b"), false);
  defs.set("a", () => SKIP);
  flow.invalidate("a");
  await flow.idle();
  assert.deepEqual(statuses("a", "b", "c"), ["skipped", "skipped", "skipped"]);
});

test("manual nodes: keep their result (stale) when an input is skipped, and skip when run", async () => {
  const { flow, define, defs, values, runs } = sheet();
  define("a", [], () => 1);
  define("m", ["a"], (a) => a * 100, { autorun: false });
  define("after", ["m"], (m) => m + 1);
  flow.runAll();
  await flow.idle();
  assert.equal(values.get("m"), 100);

  runs.length = 0;
  defs.set("a", () => SKIP);
  flow.invalidate("a");
  await flow.idle();
  assert.deepEqual(runs, ["a"]);
  assert.equal(flow.get("m").status, "done", "manual nodes stay put");
  assert.equal(flow.get("m").stale, true);
  assert.equal(flow.get("after").status, "done");

  flow.run("m");
  await flow.idle();
  assert.deepEqual(runs, ["a"], "m was skipped without running");
  assert.equal(flow.get("m").status, "skipped");
  assert.equal(flow.get("m").stale, false);
  assert.equal(flow.get("after").status, "skipped", "autorun dependents follow");

  // The input produces a value again: the skipped manual node is stale, not rerun.
  defs.set("a", () => 2);
  flow.invalidate("a");
  await flow.idle();
  assert.equal(flow.get("m").status, "skipped");
  assert.equal(flow.get("m").stale, true);
  flow.run("m");
  await flow.idle();
  assert.equal(values.get("m"), 200);
  assert.equal(values.get("after"), 201);
});

test("invalidating a skipped manual node marks it stale", async () => {
  const { flow, define } = sheet();
  define("m", [], () => SKIP, { autorun: false });
  flow.run("m");
  await flow.idle();
  flow.invalidate("m");
  assert.equal(flow.get("m").status, "skipped");
  assert.equal(flow.get("m").stale, true);
});

test("a never-run manual node with a skipped input stays idle until asked", async () => {
  const { flow, define } = sheet();
  define("a", [], () => SKIP);
  define("m", ["a"], (a) => a, { autorun: false });
  flow.runAll({ all: false });
  await flow.idle();
  assert.equal(flow.get("m").status, "idle");
  flow.run("m");
  await flow.idle();
  assert.equal(flow.get("m").status, "skipped");
});

test("a node waiting for an input that doesn't exist yet is skipped when that input arrives skipped", async () => {
  const { flow, define } = sheet();
  define("b", ["a"], (a) => a);
  flow.run("b");
  await flow.idle();
  assert.equal(flow.get("b").status, "waiting");
  define("a", [], () => SKIP);
  flow.run("a");
  await flow.idle();
  assert.equal(flow.get("b").status, "skipped");
});

test("cycles: a skipped node pulled into a cycle errors, loses its skip info, and is released to idle", async () => {
  const { flow, define } = sheet();
  define("a", [], () => SKIP);
  define("b", ["a"], (a) => a);
  flow.runAll();
  await flow.idle();
  define("a", ["b"], (b) => b);
  assert.equal(flow.get("a").status, "error");
  assert.equal(flow.get("a").cycle, true);
  assert.equal("skip" in flow.get("a"), false);
  define("a", [], () => SKIP);
  assert.equal(flow.get("a").status, "idle");
  flow.runAll();
  await flow.idle();
  assert.equal(flow.get("b").status, "skipped");
});

test("a consumer downstream of a cycle waits, it doesn't treat the cycle as a skip", async () => {
  const { flow, define, runs } = sheet();
  define("x", ["y"], (y) => y);
  define("y", ["x"], (x) => x);
  define("co", ["x"], coalesce, { consumesSkip: true });
  flow.runAll();
  await flow.idle();
  assert.equal(flow.get("co").status, "waiting");
  assert.ok(!runs.includes("co"));
});

test("runAll keeps dependency order and never runs a skipped dependent", async () => {
  const { flow, define, runs } = sheet();
  define("root", [], () => 1);
  define("gate", ["root"], () => SKIP);
  define("left", ["root"], (r) => r + 1);
  define("behind", ["gate"], (g) => g);
  define("both", ["left", "behind"], (l, b) => l + b);
  define("co", ["left", "behind"], coalesce, { consumesSkip: true });
  flow.runAll();
  await flow.idle();
  assert.deepEqual(runs.slice(0, 1), ["root"]);
  assert.ok(runs.indexOf("left") < runs.indexOf("co"));
  assert.ok(!runs.includes("behind") && !runs.includes("both"));
  assert.equal(flow.get("co").result, 2);
});

test("reset forgets skips", async () => {
  const { flow, define } = sheet();
  define("a", [], () => SKIP);
  define("b", ["a"], (a) => a);
  flow.runAll();
  await flow.idle();
  flow.reset();
  for (const id of ["a", "b"]) {
    assert.equal(flow.get(id).status, "idle");
    assert.equal("skip" in flow.get(id), false);
  }
});

test("deleting a skipped node makes its dependents wait", async () => {
  const { flow, define } = sheet();
  define("a", [], () => SKIP);
  define("b", ["a"], (a) => a);
  flow.runAll();
  await flow.idle();
  flow.delete("a");
  assert.equal(flow.get("b").status, "waiting");
  assert.equal("skip" in flow.get("b"), false);
});

test("changing consumesSkip with set takes effect on the next run", async () => {
  const { flow, define, runs } = sheet();
  define("a", [], () => SKIP);
  define("b", ["a"], () => "ran anyway");
  flow.runAll();
  await flow.idle();
  assert.equal(flow.get("b").status, "skipped");
  flow.set("b", { deps: ["a"], consumesSkip: true });
  flow.invalidate("b");
  await flow.idle();
  assert.ok(runs.includes("b"));
  assert.equal(flow.get("b").result, "ran anyway");
  flow.set("b", { deps: ["a"] });
  assert.equal(flow.consumesSkip("b"), false, "set without the option clears it, like autorun");
});

test("subscribe sees skipped transitions", async () => {
  const flow = createDataflow({ run: (id) => (id === "a" ? SKIP : id) });
  const seen = [];
  flow.subscribe((id, s) => seen.push(`${id}:${s.status}`));
  flow.set("a").set("b", { deps: ["a"] });
  flow.runAll();
  await flow.idle();
  assert.deepEqual(seen, ["a:idle", "b:idle", "a:running", "a:skipped", "b:skipped"]);
});

test("the default path is unchanged: no skip field, an empty ctx.skipped, consumesSkip false", async () => {
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
  assert.ok(states.every((s) => !("skip" in s)));
  assert.ok(contexts.every((c) => Array.isArray(c.skipped) && c.skipped.length === 0));
  assert.equal(flow.consumesSkip("a"), false);
  assert.equal(flow.consumesSkip("missing"), undefined);
});
