// Two ways to run less in a notebook whose cells are expensive (an LLM call,
// a paged fetch):
//
// 1. A held run. `run(id, { follow: false })` runs one cell and holds its
//    dependents: they keep their results and are flagged `stale` instead of
//    rerunning. `runAll({ ids, follow: false })` runs a chosen set (say, a
//    cell and what it needs) and holds everything below it.
// 2. An equality cut-off. The values are yours, so the comparison is too:
//    when a cell's new value equals its old one, `run` returns UNCHANGED and
//    nothing downstream reruns.
import assert from "node:assert/strict";
import { createDataflow, UNCHANGED } from "@johnhenry/dataflow";

const values = new Map(); // yours, not the scheduler's
const formulas = {
  topic: () => "otters",
  prompt: (topic) => `Write one sentence about ${topic}.`,
  answer: (prompt) => `(an expensive LLM call for "${prompt}")`,
  shown: (answer) => answer.toUpperCase(),
};
const ran = [];

const flow = createDataflow({
  async run(id, { deps, signal }) {
    ran.push(id);
    const value = await formulas[id](...deps.map((d) => values.get(d)));
    if (signal.aborted) return value;
    if (values.has(id) && values.get(id) === value) return UNCHANGED; // the cut-off
    values.set(id, value);
    return value;
  },
});

flow
  .set("topic")
  .set("prompt", { deps: ["topic"] })
  .set("answer", { deps: ["prompt"] })
  .set("shown", { deps: ["answer"] });
flow.runAll();
await flow.idle();

// Edit the prompt and look at it, without paying for the LLM call yet.
formulas.prompt = (topic) => `Write a haiku about ${topic}.`;
ran.length = 0;
flow.run("prompt", { follow: false });
await flow.idle();
assert.deepEqual(ran, ["prompt"]);
assert.equal(flow.get("answer").stale, true, "the LLM cell is flagged stale, with its old result");
assert.match(values.get("answer"), /one sentence/);

// Now run it (autorun dependents follow as usual).
flow.run("answer");
await flow.idle();
assert.deepEqual(ran, ["prompt", "answer", "shown"]);
assert.equal(flow.get("answer").stale, false);

// The topic is retyped with the same text: the cut-off stops right there.
ran.length = 0;
formulas.topic = () => "otters";
flow.invalidate("topic");
await flow.idle();
assert.deepEqual(ran, ["topic"]);
assert.equal(flow.get("topic").unchanged, true);
console.log("ok: a held run flags the expensive cell stale; an unchanged value stops propagation");
