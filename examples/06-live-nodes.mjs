// Live nodes: a run that settles more than once.
//
// 1. A live source. `run` returns `live(iterable)` (here an async generator
//    reading a stand-in for a socket or a sensor). Each item settles the node
//    and its dependents run; the next item is pulled only once they settled,
//    so a burst of readings is shown one by one, never superseded. Live runs
//    are opt-in: `run` wraps the generator in `live(...)` (any other async
//    iterable it returned would be a plain value).
// 2. Restart and cleanup. Changing the unit restarts the live run: its signal
//    aborts, the generator's `finally` runs, and a new run starts.
// 3. Resume. A node set with `resume: true` keeps its live run when its input
//    changes and is told through `ctx.changes`: here, a history that
//    accumulates every reading shown, which a restart would have lost.
import assert from "node:assert/strict";
import { createDataflow, live } from "@johnhenry/dataflow";

// A stand-in for a socket: readings arrive when `arrive` is called.
const queue = [];
let wake = () => {};
const arrive = (...celsius) => (queue.push(...celsius), wake());
async function* readings(signal) {
  while (!signal.aborted) {
    if (queue.length) yield queue.shift();
    else await new Promise((resolve) => ((wake = resolve), signal.addEventListener("abort", resolve, { once: true })));
  }
}

const values = new Map(); // yours, not the scheduler's
const log = [];
let unit = "C";
const formulas = {
  unit: () => unit,
  async *temperature(unit, { signal }) {
    try {
      for await (const c of readings(signal)) yield unit === "F" ? (c * 9) / 5 + 32 : c;
    } finally {
      log.push(`closed the ${unit} stream`);
    }
  },
  shown: async (t, unit) => (await null, `${t}°${unit}`),
  async *history(shown, ctx) {
    const seen = [shown];
    yield seen;
    for await (const _change of ctx.changes) yield (seen.push(values.get("shown")), seen);
  },
};

// Values are stored as they settle: per item for a live run.
async function* storing(id, items, signal) {
  for await (const item of items) {
    if (signal.aborted) return;
    values.set(id, item);
    yield item;
  }
}
const flow = createDataflow({
  async run(id, ctx) {
    const out = await formulas[id](...ctx.deps.map((d) => values.get(d)), ctx);
    if (Object.prototype.toString.call(formulas[id]) === "[object AsyncGeneratorFunction]") return live(storing(id, out, ctx.signal));
    if (!ctx.signal.aborted) values.set(id, out);
    if (id === "shown") log.push(`shown ${out}`);
    return out;
  },
});

flow
  .set("unit")
  .set("temperature", { deps: ["unit"] })
  .set("shown", { deps: ["temperature", "unit"] })
  .set("history", { deps: ["shown"], resume: true });
flow.runAll();

// 1. A burst of readings: each is shown, in order, before the next is pulled.
arrive(20, 21, 23);
await flow.idle();
assert.deepEqual(log, ["shown 20°C", "shown 21°C", "shown 23°C"]);
assert.equal(flow.get("temperature").live, true);
assert.equal(flow.get("temperature").version, 3);
assert.equal(flow.get("temperature").runId, 1, "one run, three settles");

// 2. A new unit restarts the stream; the old generator's finally runs. The new
//    run is `running` until its first reading (so `idle()` would wait for it):
//    wait for it to open instead, then send a reading.
const until = (test) => new Promise((resolve) => {
  const off = flow.subscribe((id, state) => test(id, state) && (off(), resolve()));
});
log.length = 0;
unit = "F";
flow.invalidate("unit");
await until((id, state) => id === "temperature" && state.live && state.runId === 2);
arrive(25);
await flow.idle();
assert.deepEqual(log, ["closed the C stream", "shown 77°F"]);
assert.equal(flow.get("temperature").runId, 2);

// 3. The history was resumed, never restarted: it kept every reading.
assert.equal(flow.get("history").runId, 1);
assert.deepEqual(values.get("history"), ["20°C", "21°C", "23°C", "77°F"]);

flow.reset(); // ends every live run
await flow.idle();
assert.deepEqual(log, ["closed the C stream", "shown 77°F", "closed the F stream"]);
console.log("ok: live readings are shown one by one, a restart closes the stream, a resumed history keeps them all");
