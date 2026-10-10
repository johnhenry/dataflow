import { test } from "node:test";
import assert from "node:assert/strict";
import { createDataflow, SKIP, skip, live } from "../src/index.mjs";

// No wall-clock sleeps: a "tick" is one turn of the event loop, and slow runs
// are held open by hand through deferreds.
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

/** Values live outside the scheduler; `defs` maps id -> (inputs..., ctx) => value. */
function sheet() {
  const values = new Map();
  const defs = new Map();
  const runs = [];
  const contexts = [];
  const flow = createDataflow({
    async run(id, ctx) {
      runs.push(id);
      contexts.push([id, ctx]);
      const value = await defs.get(id)(...ctx.deps.map((d) => values.get(d)), ctx);
      if (!ctx.signal.aborted) values.set(id, value);
      return value;
    },
  });
  const define = (id, deps, fn, opts = {}) => {
    defs.set(id, fn);
    flow.set(id, { deps, ...opts });
  };
  const ctxOf = (id) => contexts.filter(([i]) => i === id).map(([, c]) => c);
  return { flow, values, defs, runs, contexts, ctxOf, define };
}

// ---------------------------------------------------------------- sample

test("sample: readers, ctx.deps lists sampled ids after deps, ctx.sampled names them, an id in both triggers", async () => {
  const { flow, define, ctxOf } = sheet();
  define("t", [], () => "t");
  define("s", [], () => "s");
  define("n", ["t"], (t, s) => `${t}+${s}`, { sample: ["s", "t", "s"] });
  assert.deepEqual(flow.deps("n"), ["t"]);
  assert.deepEqual(flow.samples("n"), ["s"], "deduplicated; `t` is in deps, so it triggers");
  assert.deepEqual(flow.samples("t"), []);
  assert.equal(flow.samples("missing"), undefined);
  assert.deepEqual(flow.dependents("t"), ["n"]);
  assert.deepEqual(flow.dependents("s"), [], "a sampler isn't a dependent: s doesn't trigger it");
  assert.deepEqual(flow.order(["n"]), ["n"]);
  assert.ok(flow.order().indexOf("s") < flow.order().indexOf("n"), "a sampled input orders the node");
  flow.runAll();
  await flow.idle();
  const [ctx] = ctxOf("n");
  assert.deepEqual(ctx.deps, ["t", "s"]);
  assert.deepEqual(ctx.sampled, ["s"]);
  assert.deepEqual(ctx.skipped, []);
  assert.equal(flow.get("n").result, "t+s");
  assert.deepEqual(ctxOf("t")[0].sampled, [], "every run has ctx.sampled");
  assert.throws(() => flow.set("x", { sample: "s" }), TypeError);
});

test("sample: a new settle of a sampled input doesn't run, stale or invalidate the node; a trigger reads its latest value", async () => {
  const { flow, define, runs, values } = sheet();
  let price = 10;
  define("price", [], () => price);
  define("click", [], () => "click");
  define("order", ["click"], (_click, p) => `ordered at ${p}`, { sample: ["price"] });
  define("manual", ["click"], (_click, p) => p, { sample: ["price"], autorun: false });
  flow.runAll();
  await flow.idle();
  assert.equal(values.get("order"), "ordered at 10");
  const before = { order: flow.get("order"), manual: flow.get("manual") };
  runs.length = 0;
  for (price of [11, 12, 13]) {
    flow.invalidate("price");
    await flow.idle();
  }
  assert.deepEqual(runs, ["price", "price", "price"], "only the sampled input ran");
  assert.deepEqual(flow.get("order"), before.order, "not run, not stale, same version and runId");
  assert.deepEqual(flow.get("manual"), before.manual, "a manual sampler isn't flagged stale either");
  flow.invalidate("click");
  await flow.idle();
  assert.equal(values.get("order"), "ordered at 13", "the trigger read the latest sampled value");
  assert.equal(flow.get("manual").stale, true, "a manual node is still flagged stale by its trigger");
});

test("sample: readiness -- the node waits until each sampled input settled once, then runs (it was asked to)", async () => {
  const { flow, define, runs } = sheet();
  define("click", [], () => "click");
  define("price", [], () => 42, { autorun: false });
  define("order", ["click"], (_c, p) => `at ${p}`, { sample: ["price"] });
  flow.run("click");
  await flow.idle();
  assert.equal(flow.get("order").status, "waiting", "the sampled input never ran: the node waits");
  assert.deepEqual(runs, ["click"]);
  flow.run("price");
  await flow.idle();
  assert.equal(flow.get("order").status, "done");
  assert.equal(flow.get("order").result, "at 42");
  assert.equal(flow.get("order").version, 1);
  // A node that was never asked to run isn't run by a sampled input settling.
  define("idleSampler", ["click"], (_c, p) => p, { sample: ["price"] });
  flow.run("price");
  await flow.idle();
  assert.equal(flow.get("idleSampler").status, "idle");
});

test("sample: a failing or waiting sampled input makes the node wait at its next trigger, and it runs once the input recovers", async () => {
  const { flow, define, defs, runs } = sheet();
  define("up", [], () => 1);
  define("price", ["up"], (u) => u * 10);
  define("click", [], () => "click");
  define("order", ["click"], (_c, p) => `at ${p}`, { sample: ["price"] });
  flow.runAll();
  await flow.idle();
  const done = flow.get("order");
  defs.set("price", () => {
    throw new Error("feed down");
  });
  flow.invalidate("price");
  await flow.idle();
  assert.equal(flow.get("price").status, "error");
  assert.deepEqual(flow.get("order"), done, "an error settle doesn't touch the sampler either");
  flow.invalidate("click");
  await flow.idle();
  assert.equal(flow.get("order").status, "waiting", "triggered while its sampled input fails: it waits");
  runs.length = 0;
  defs.set("price", (u) => u * 100);
  flow.invalidate("price");
  await flow.idle();
  assert.deepEqual(runs, ["price", "order"]);
  assert.equal(flow.get("order").result, "at 100");
  // A sampled input that is itself waiting (its own input failed) gates the same way.
  defs.set("up", () => Promise.reject(new Error("up down")));
  flow.invalidate("up");
  await flow.idle();
  assert.equal(flow.get("price").status, "waiting");
  assert.equal(flow.get("order").status, "done", "the failure doesn't cascade through a sampled edge");
  flow.invalidate("click");
  await flow.idle();
  assert.equal(flow.get("order").status, "waiting");
  defs.set("up", () => 2);
  flow.invalidate("up");
  await flow.idle();
  assert.equal(flow.get("order").result, "at 200");
});

test("sample: glitch-free -- in a sampled diamond the node runs once, after the sampled branch, on one consistent version", async () => {
  const { flow, define, values } = sheet();
  const seen = [];
  let a = 1;
  define("a", [], () => a);
  // The sampled branch is slow, the triggering path fast: a glitch would show.
  define("slow", ["a"], async (v, ctx) => (await ticks(4), ctx.signal.aborted ? undefined : v * 10));
  define("join", ["a"], (v, s) => {
    seen.push([v, s]);
    return v + s;
  }, { sample: ["slow"] });
  flow.runAll();
  await flow.idle();
  for (a of [2, 3]) {
    flow.invalidate("a");
    await flow.idle();
  }
  assert.deepEqual(seen, [
    [1, 10],
    [2, 20],
    [3, 30],
  ]);
  assert.equal(flow.get("join").version, 3, "once per change of a, never for slow's settle");
  // The node never starts while a sampled input is running.
  const gate = deferred();
  define("slow", ["a"], () => gate.promise.then(() => values.get("a") * 1000));
  flow.run("slow");
  await tick();
  flow.run("join");
  await ticks(5);
  assert.equal(flow.get("slow").status, "running");
  assert.notEqual(flow.get("join").status, "running", "held behind its running sampled input");
  gate.resolve();
  await flow.idle();
  assert.deepEqual(seen.at(-1), [3, 3000]);
});

test("sample: a skipped sampled input spreads its skip when the node is triggered, unless the node consumes it; its skip settle doesn't spread", async () => {
  const { flow, define, defs, ctxOf } = sheet();
  define("s", [], () => "s");
  define("t", [], () => "t");
  define("n", ["t"], (t, s) => `${t}+${s}`, { sample: ["s"] });
  define("c", ["t"], (t, s) => `${t}+${s ?? "none"}`, { sample: ["s"], consumesSkip: ["s"] });
  flow.runAll();
  await flow.idle();
  defs.set("s", () => skip("off"));
  flow.invalidate("s");
  await flow.idle();
  assert.equal(flow.get("s").status, "skipped");
  assert.equal(flow.get("n").status, "done", "the skip settle didn't spread through the sampled edge");
  flow.invalidate("t");
  await flow.idle();
  assert.equal(flow.get("n").status, "skipped", "triggered: the skipped sampled input spreads");
  assert.deepEqual(flow.get("n").skip, { source: "s", reason: "off" });
  assert.equal(flow.get("c").status, "done", "a consumer runs anyway");
  assert.deepEqual(ctxOf("c").at(-1).skipped, ["s"]);
});

test("sample: a cycle through a sampled edge is a cycle; a self-sample too", () => {
  const flow = createDataflow({ run: () => 1 });
  flow.set("a", { sample: ["b"] }).set("b", { deps: ["a"] });
  assert.equal(flow.get("a").cycle, true);
  assert.equal(flow.get("b").cycle, true);
  flow.set("a");
  assert.equal(flow.get("a").cycle, false);
  flow.set("self", { sample: ["self"] });
  assert.equal(flow.get("self").cycle, true);
});

test("sample: a live sampled source settles freely (its items don't trigger and don't wait for the sampler)", async () => {
  const runs = [];
  const values = new Map();
  let pulls = 0;
  const flow2 = createDataflow({
    run(id, ctx) {
      runs.push(id);
      if (id === "src")
        return live(
          (async function* () {
            for (let i = 1; i <= 5; i++) {
              pulls++;
              values.set("src", i);
              yield i;
            }
          })(),
        );
      return `read ${values.get("src")}`;
    },
  });
  flow2.set("src").set("click").set("reader", { deps: ["click"], sample: ["src"] });
  flow2.runAll();
  await flow2.idle();
  assert.equal(pulls, 5, "every item pulled; the sampler never held the source");
  assert.equal(flow2.get("src").version, 5);
  assert.equal(runs.filter((id) => id === "reader").length, 1, "the sampler ran once (runAll), not per item");
  assert.equal(flow2.get("reader").result, "read 1", "it ran on the first item (glitch-free: after src settled once)");
  flow2.run("click");
  await flow2.idle();
  assert.equal(flow2.get("reader").result, "read 5");
});

test("sample with resume: a sampled input's settle doesn't notify a live resume run", async () => {
  const notices = [];
  const flow = createDataflow({
    run(id, ctx) {
      if (id !== "r") return id;
      ctx.onChange((n) => notices.push(n));
      ctx.emit("ready");
      return new Promise(() => {});
    },
  });
  flow.set("t").set("s").set("r", { deps: ["t"], sample: ["s"], resume: true });
  flow.runAll();
  await flow.idle();
  flow.invalidate("s");
  await flow.idle();
  assert.deepEqual(notices, []);
  flow.invalidate("t");
  await flow.idle();
  assert.deepEqual(notices, [{ deps: ["t", "s"], skipped: [], changed: ["t"] }]);
  assert.equal(flow.get("r").runId, 1);
  flow.reset();
});

test("sample with follow: false -- a held run of the trigger flags the sampler stale; a held run of a sampled input doesn't", async () => {
  const { flow, define, runs } = sheet();
  define("t", [], () => "t");
  define("s", [], () => "s");
  define("n", ["t"], (t, s) => t + s, { sample: ["s"] });
  flow.runAll();
  await flow.idle();
  runs.length = 0;
  flow.run("s", { follow: false });
  await flow.idle();
  assert.equal(flow.get("n").stale, false);
  flow.run("t", { follow: false });
  await flow.idle();
  assert.equal(flow.get("n").stale, true);
  assert.deepEqual(runs, ["s", "t"]);
});

// ---------------------------------------------------------------- queue

/** A queue node `q` behind an emitting source `src`; `q`'s runs are held open by hand. */
function queued(opts = {}) {
  const gates = [];
  const log = [];
  let emit;
  const flow = createDataflow({
    run(id, ctx) {
      if (id === "src") {
        emit = ctx.emit;
        return new Promise(() => {}); // stays live, settles through emits
      }
      if (id === "q") {
        const gate = deferred();
        gates.push(gate);
        log.push(`start ${ctx.trigger.type}:${ctx.trigger.id ?? ""}:${ctx.trigger.result ?? ""} #${ctx.runId}`);
        ctx.signal.addEventListener("abort", () => log.push(`abort #${ctx.runId}`));
        return gate.promise.then((v) => (log.push(`end #${ctx.runId}`), v ?? ctx.trigger.result));
      }
      log.push(`${id} ran`);
      return id;
    },
  });
  flow.set("src").set("q", { deps: ["src"], queue: true, ...opts });
  return { flow, gates, log, emit: (v) => emit(v) };
}
const finishAll = async ({ flow, gates }) => {
  for (let i = 0; i < 100 && (flow.get("q").status === "running" || flow.get("q").queued > 0); i++) {
    gates.at(-1)?.resolve();
    await ticks(2);
  }
};

test("queue: each trigger queues; runs go one after another, in order, none aborted; ctx.trigger says which settle", async () => {
  const s = queued();
  const { flow, gates, log } = s;
  assert.equal(flow.queues("q"), true);
  assert.equal(flow.queues("src"), false);
  assert.equal(flow.queueLimit("q"), 1000);
  flow.run("src");
  await ticks(2);
  s.emit("e1");
  await ticks(2);
  assert.equal(flow.get("q").status, "running");
  s.emit("e2");
  s.emit("e3");
  s.emit("e4");
  await ticks(2);
  assert.equal(flow.get("q").queued, 3, "three triggers wait behind the run in flight");
  assert.equal(gates.length, 1, "no new run started");
  await finishAll(s);
  assert.deepEqual(log, [
    "start settle:src:e1 #1",
    "end #1",
    "start settle:src:e2 #2",
    "end #2",
    "start settle:src:e3 #3",
    "end #3",
    "start settle:src:e4 #4",
    "end #4",
  ]);
  assert.equal(flow.get("q").version, 4);
  assert.equal(flow.get("q").queued, 0);
  assert.equal(flow.get("q").dropped, 0);
  flow.reset();
});

test("queue: ctx.trigger carries the settle's id, status, version and result reference, frozen", async () => {
  const triggers = [];
  let emit;
  const flow = createDataflow({
    run(id, ctx) {
      if (id === "src") {
        emit = ctx.emit;
        return new Promise(() => {});
      }
      triggers.push(ctx.trigger);
    },
  });
  flow.set("src").set("q", { deps: ["src"], queue: true, consumesSkip: true });
  flow.run("src");
  await tick();
  const payload = { big: "object" };
  emit(payload);
  emit(skip("nothing"));
  await flow.idle();
  flow.run("q");
  flow.invalidate("q");
  await flow.idle();
  assert.equal(triggers.length, 4);
  assert.deepEqual(triggers[0], { type: "settle", id: "src", status: "done", version: 1, result: payload });
  assert.equal(triggers[0].result, payload, "the same reference: the scheduler copies nothing");
  assert.deepEqual(triggers[1], { type: "settle", id: "src", status: "skipped", version: 2, result: undefined, skip: { source: "src", reason: "nothing" } });
  assert.deepEqual(triggers[2], { type: "run" });
  assert.deepEqual(triggers[3], { type: "invalidate" });
  assert.ok(triggers.every((t) => Object.isFrozen(t)));
  flow.reset();
});

test("queue: explicit runs queue behind the run in flight instead of superseding it", async () => {
  const s = queued();
  const { flow, log } = s;
  flow.run("src");
  await ticks(2);
  s.emit("e1");
  await ticks(2);
  flow.run("q").run("q");
  await ticks(2);
  assert.equal(flow.get("q").queued, 2);
  await finishAll(s);
  assert.deepEqual(log, ["start settle:src:e1 #1", "end #1", "start run:: #2", "end #2", "start run:: #3", "end #3"]);
  assert.ok(!log.some((l) => l.startsWith("abort")));
  flow.reset();
});

test("queue: past queueLimit the oldest trigger is dropped and counted in state.dropped", async () => {
  const s = queued({ queueLimit: 2 });
  const { flow, log } = s;
  assert.equal(flow.queueLimit("q"), 2);
  flow.run("src");
  await ticks(2);
  s.emit("e1");
  await ticks(2); // e1's run is in flight
  for (const v of ["e2", "e3", "e4", "e5", "e6"]) s.emit(v);
  await ticks(2);
  assert.equal(flow.get("q").status, "running");
  assert.equal(flow.get("q").queued, 2);
  assert.equal(flow.get("q").dropped, 3, "e2, e3 and e4 were dropped, oldest first");
  await finishAll(s);
  assert.deepEqual(
    log.filter((l) => l.startsWith("start")),
    ["start settle:src:e1 #1", "start settle:src:e5 #2", "start settle:src:e6 #3"],
  );
  // A lower limit set later trims the queue the same way.
  flow.set("q", { deps: ["src"], queue: true, queueLimit: 1 });
  assert.equal(flow.queueLimit("q"), 1);
  assert.throws(() => flow.set("q", { queue: true, queueLimit: 0 }), RangeError);
  assert.throws(() => flow.set("q", { queue: true, queueLimit: 1.5 }), RangeError);
  flow.set("unbounded", { queue: true, queueLimit: Infinity });
  flow.reset();
});

test("queue: a live source feeding a queue node delivers every value, in order (pull form with backpressure, and emit form without)", async () => {
  const got = [];
  const flow = createDataflow({
    run(id, ctx) {
      if (id === "pull") {
        return live(
          (async function* () {
            for (let i = 1; i <= 6; i++) yield `p${i}`;
          })(),
        );
      }
      if (id === "push") {
        for (let i = 1; i <= 6; i++) ctx.emit(`e${i}`); // a burst: no backpressure
        ctx.close();
        return;
      }
      if (id === "plain") return id;
      return (async () => {
        await ticks(1 + (ctx.runId % 3)); // uneven speeds
        got.push(`${id}<-${ctx.trigger.result}`);
      })();
    },
  });
  flow.set("pull").set("push");
  flow.set("qPull", { deps: ["pull"], queue: true }).set("qPush", { deps: ["push"], queue: true });
  // For contrast, a superseding node behind the burst only sees the last emit.
  const plain = [];
  flow.subscribe((id, s) => id === "plain" && s.status === "done" && plain.push(s.version));
  flow.set("plain", { deps: ["push"] });
  flow.run("pull").run("push"); // runAll would also queue a `run` trigger on each queue node
  await flow.idle();
  assert.deepEqual(got.filter((g) => g.startsWith("qPull")), ["p1", "p2", "p3", "p4", "p5", "p6"].map((v) => `qPull<-${v}`));
  assert.deepEqual(got.filter((g) => g.startsWith("qPush")), ["e1", "e2", "e3", "e4", "e5", "e6"].map((v) => `qPush<-${v}`));
  assert.equal(flow.get("qPush").dropped, 0);
  assert.equal(flow.get("qPush").version, 6);
  assert.equal(plain.length, 1, "the superseding node coalesced the burst");
});

test("queue: a queue node's own live run holds its queue until it ends; the live source upstream is held too (backpressure)", async () => {
  const log = [];
  let pulls = 0;
  const ends = [];
  const flow = createDataflow({
    run(id, ctx) {
      if (id === "src") {
        return live(
          (async function* () {
            for (let i = 1; i <= 3; i++) {
              pulls++;
              yield i;
            }
          })(),
        );
      }
      log.push(`start ${ctx.trigger.result}`);
      const end = deferred();
      ends.push(() => (ctx.emit(`last ${ctx.trigger.result}`), end.resolve()));
      ctx.emit(`first ${ctx.trigger.result}`);
      return end.promise;
    },
  });
  flow.set("src").set("q", { deps: ["src"], queue: true });
  flow.run("src");
  await ticks(10);
  assert.equal(flow.get("q").live, true);
  assert.deepEqual(log, ["start 1"]);
  assert.ok(pulls <= 2, `the source is held while q has work queued (pulls=${pulls})`);
  for (let i = 0; i < 3; i++) {
    ends.at(-1)();
    await ticks(10);
  }
  assert.deepEqual(log, ["start 1", "start 2", "start 3"]);
  assert.equal(pulls, 3);
  assert.equal(flow.get("q").result, "last 3");
  assert.equal(flow.get("q").live, false);
});

test("queue: run(id, { replace: true }) drops what is queued and supersedes the run in flight", async () => {
  const s = queued();
  const { flow, log } = s;
  flow.run("src");
  await ticks(2);
  s.emit("e1");
  await ticks(2);
  s.emit("e2");
  s.emit("e3");
  await ticks(1);
  assert.equal(flow.get("q").queued, 2);
  flow.run("q", { replace: true });
  await ticks(3);
  assert.equal(flow.get("q").queued, 0);
  await finishAll(s);
  assert.deepEqual(log, ["start settle:src:e1 #1", "abort #1", "start run:: #2", "end #2"]);
  assert.equal(flow.get("q").dropped, 0, "a replace isn't a drop");
  // On a node without `queue`, replace changes nothing: it always supersedes.
  const flow2 = createDataflow({ run: (id) => id });
  flow2.set("x");
  flow2.run("x", { replace: true });
  await flow2.idle();
  assert.equal(flow2.get("x").result, "x");
  assert.equal(flow2.get("x").queued, undefined);
});

test("queue: reset() and delete(id) clear the queue", async () => {
  const s = queued();
  const { flow, log } = s;
  flow.run("src");
  await ticks(2);
  s.emit("e1");
  await ticks(2);
  s.emit("e2");
  s.emit("e3");
  await ticks(1);
  assert.equal(flow.get("q").queued, 2);
  flow.reset();
  assert.equal(flow.get("q").status, "idle");
  assert.equal(flow.get("q").queued, 0);
  assert.equal(flow.get("q").dropped, 0);
  await ticks(5);
  assert.deepEqual(log, ["start settle:src:e1 #1", "abort #1"], "nothing ran after the reset");
  flow.run("src");
  await ticks(2);
  s.emit("e4");
  await ticks(2);
  s.emit("e5");
  await ticks(1);
  flow.delete("q");
  await ticks(5);
  assert.equal(log.filter((l) => l.startsWith("start")).length, 2, "the run for e5 never started");
  assert.equal(flow.has("q"), false);
  flow.reset();
});

test("queue with follow: false -- each trigger keeps its own follow; a held settle upstream doesn't trigger a done queue node", async () => {
  const runs = [];
  const flow = createDataflow({
    run(id, ctx) {
      runs.push(`${id}${ctx.trigger ? ` ${ctx.trigger.type}` : ""}`);
      return id;
    },
  });
  flow.set("up").set("q", { deps: ["up"], queue: true }).set("down", { deps: ["q"] });
  flow.runAll();
  await flow.idle();
  runs.length = 0;
  flow.run("q", { follow: false }).run("q");
  await flow.idle();
  assert.deepEqual(runs, ["q run", "q run", "down"], "the held run held down; the second followed");
  runs.length = 0;
  flow.run("q", { follow: false });
  await flow.idle();
  assert.equal(flow.get("down").stale, true);
  runs.length = 0;
  flow.run("up", { follow: false });
  await flow.idle();
  assert.deepEqual(runs, ["up"]);
  assert.equal(flow.get("q").stale, true, "flagged stale, like any dependent of a held run");
  assert.equal(flow.get("q").queued, 0, "no trigger was queued");
});

test("queue with resume: the two can't be combined; a resume live run upstream delivers every settle to a queue node", async () => {
  const flow0 = createDataflow({ run: () => 1 });
  assert.throws(() => flow0.set("x", { queue: true, resume: true }), TypeError);
  const got = [];
  let x = 0;
  const flow = createDataflow({
    async *run(id, ctx) {
      if (id === "x") {
        yield ++x;
        return;
      }
      if (id === "acc") {
        let total = 0;
        yield (total += x);
        for await (const _ of ctx.changes) yield (total += x);
        return;
      }
      await ticks(2);
      got.push(ctx.trigger.result);
      yield ctx.trigger.result;
    },
  });
  flow.set("x").set("acc", { deps: ["x"], resume: true }).set("log", { deps: ["acc"], queue: true });
  flow.run("x");
  await flow.idle();
  for (let i = 0; i < 3; i++) {
    flow.invalidate("x");
    await flow.idle();
  }
  assert.equal(flow.get("acc").runId, 1, "acc resumed, never restarted");
  assert.deepEqual(got, [1, 3, 6, 10], "every running total reached the queue node, in order");
  flow.reset();
});

test("queue: an input failing mid-run puts the interrupted trigger back at the head; it runs once the input recovers", async () => {
  const log = [];
  const gates = [];
  let emit;
  let brokenRun = false;
  const flow = createDataflow({
    run(id, ctx) {
      if (id === "src") {
        emit = ctx.emit;
        return new Promise(() => {});
      }
      if (id === "cfg") {
        if (brokenRun) throw new Error("cfg broke");
        return "cfg";
      }
      const gate = deferred();
      gates.push(gate);
      log.push(`start ${ctx.trigger.result ?? ctx.trigger.type}`);
      ctx.signal.addEventListener("abort", () => log.push(`abort ${ctx.trigger.result}`));
      return gate.promise.then(() => ctx.signal.aborted || log.push(`end ${ctx.trigger.result ?? ctx.trigger.type}`));
    },
  });
  flow.set("src").set("cfg");
  flow.run("cfg");
  await flow.idle();
  flow.set("q", { deps: ["src", "cfg"], queue: true });
  flow.run("src");
  await ticks(3);
  emit("e1");
  await ticks(2);
  emit("e2");
  await ticks(1);
  brokenRun = true;
  flow.run("cfg");
  await ticks(3);
  assert.equal(flow.get("q").status, "waiting");
  assert.equal(flow.get("q").queued, 2, "e1 went back to the head, e2 stays behind it");
  brokenRun = false;
  flow.run("cfg"); // recovers: the settle is a trigger too, queued last
  for (let i = 0; i < 20; i++) {
    gates.at(-1)?.resolve();
    await ticks(2);
  }
  assert.deepEqual(log, ["start e1", "abort e1", "start e1", "end e1", "start e2", "end e2", "start cfg", "end cfg"]);
  flow.reset();
});

test("queue: dependents of a queue node run once its queue drained (glitch-free); a queue dependent gets every settle", async () => {
  const downRuns = [];
  const qdown = [];
  let emit;
  const flow = createDataflow({
    run(id, ctx) {
      if (id === "src") {
        emit = ctx.emit;
        return new Promise(() => {});
      }
      if (id === "q") return ticks(2).then(() => `q(${ctx.trigger.result})`);
      if (id === "down") downRuns.push(flow.get("q").result);
      if (id === "qdown") qdown.push(`${ctx.trigger.version}:${ctx.trigger.result}`);
      return id;
    },
  });
  flow.set("src").set("q", { deps: ["src"], queue: true }).set("down", { deps: ["q"] }).set("qdown", { deps: ["q"], queue: true });
  flow.run("src");
  await ticks(2);
  emit("a");
  emit("b");
  emit("c");
  await flow.idle();
  assert.deepEqual(downRuns, ["q(c)"], "a superseding dependent runs once, on the last settle");
  assert.deepEqual(qdown, ["1:q(a)", "2:q(b)", "3:q(c)"], "a queue dependent sees each settle's version and result");
  flow.reset();
});

test("queue: each trigger is judged by its own settle -- a skip answers it with a skip (no run), a value queued before a skip still runs; no abort", async () => {
  const log = [];
  const gates = [];
  let emit;
  const flow = createDataflow({
    run(id, ctx) {
      if (id === "src") {
        emit = ctx.emit;
        return new Promise(() => {});
      }
      const gate = deferred();
      gates.push(gate);
      log.push(`start ${ctx.trigger.result}`);
      ctx.signal.addEventListener("abort", () => log.push("abort"));
      return gate.promise.then(() => log.push(`end ${ctx.trigger.result}`));
    },
  });
  flow.set("src").set("q", { deps: ["src"], queue: true });
  flow.run("src");
  await tick();
  emit("v1");
  await ticks(2);
  emit(SKIP); // while v1 is in flight
  emit("v2");
  emit("v3");
  emit(skip("over")); // src is skipped now, but v2 and v3 were values
  await ticks(2);
  assert.equal(flow.get("q").queued, 4);
  for (let i = 0; i < 10; i++) {
    gates.at(-1).resolve();
    await ticks(3);
  }
  assert.equal(gates.length, 3, "the skip triggers started no run");
  assert.deepEqual(log, ["start v1", "end v1", "start v2", "end v2", "start v3", "end v3"]);
  assert.equal(flow.get("q").status, "skipped", "the last trigger was a skip");
  assert.deepEqual(flow.get("q").skip, { source: "src", reason: "over" });
  assert.equal(flow.get("q").queued, 0);
  flow.reset();
});

test("queue and sample together: the Run pattern -- events trigger, values are read", async () => {
  const sent = [];
  let emitClick;
  let value = "draft 1";
  const flow = createDataflow({
    run(id, ctx) {
      if (id === "click") {
        emitClick = ctx.emit;
        return new Promise(() => {});
      }
      if (id === "text") return value;
      if (id === "send") return ticks(1).then(() => sent.push(`${ctx.trigger.result}: ${value}`));
    },
  });
  flow.set("click").set("text").set("send", { deps: ["click"], sample: ["text"], queue: true });
  // The click source hasn't fired yet, so it is `running` and idle() would
  // wait for its first event: turn the event loop instead.
  flow.run("click").run("text");
  await ticks(3);
  value = "draft 2";
  flow.invalidate("text");
  await ticks(3);
  assert.equal(flow.get("click").status, "running");
  assert.deepEqual(sent, [], "typing doesn't send");
  emitClick("click 1");
  emitClick("click 2");
  await flow.idle();
  assert.deepEqual(sent, ["click 1: draft 2", "click 2: draft 2"], "each click sends once, in order, with the text as it is");
  flow.reset();
});

test("queue: switching the option on and off with set()", async () => {
  const states = [];
  const flow = createDataflow({ run: (id) => id, onChange: (id, s) => states.push([id, s]) });
  flow.set("n");
  assert.equal("queued" in flow.get("n"), false, "a node without queue has no queue keys");
  flow.set("n", { queue: true });
  assert.equal(flow.get("n").queued, 0);
  assert.equal(flow.get("n").dropped, 0);
  flow.run("n").run("n");
  flow.set("n", { queue: false });
  assert.equal("queued" in flow.get("n"), false);
  assert.equal("dropped" in flow.get("n"), false);
  await flow.idle();
  assert.equal(flow.get("n").version, 1, "what was queued became one ordinary run");
});

test("a graph without sample or queue: no queue keys in any state, ctx.trigger absent, ctx.sampled empty", async () => {
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
  assert.ok(states.every((s) => !("queued" in s) && !("dropped" in s) && !("sampled" in s)));
  assert.ok(contexts.every((c) => !("trigger" in c) && Array.isArray(c.sampled) && c.sampled.length === 0));
  assert.equal(flow.queues("a"), false);
  assert.deepEqual(flow.samples("a"), []);
});
