// Typing fast reruns a node before its slow previous run finishes.
// The superseded run's AbortSignal fires and its result is dropped.
import assert from "node:assert/strict";
import { createDataflow } from "@johnhenry/dataflow";

const delay = (ms) => new Promise((r) => setTimeout(r, ms));
let query = "ap";
const results = new Map();
const flow = createDataflow({
  async run(id, { signal }) {
    const q = query;
    await delay(q === "ap" ? 40 : 5); // the first request is slow
    if (signal.aborted) return; // superseded: don't write
    results.set(id, `results for ${q}`);
    return q;
  },
});
flow.set("search");
flow.run("search");
await delay(5);
query = "apple";
flow.invalidate("search");
await flow.idle();
await delay(50);
assert.equal(results.get("search"), "results for apple");
assert.equal(flow.get("search").result, "apple");
console.log("ok:", results.get("search"));
