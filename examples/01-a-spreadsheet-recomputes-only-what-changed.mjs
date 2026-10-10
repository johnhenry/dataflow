// A spreadsheet: values live in a Map the scheduler never sees.
// Changing "price" recomputes "tip" and "total"; "note" (no inputs) is untouched.
import assert from "node:assert/strict";
import { createDataflow } from "@johnhenry/dataflow";

const values = new Map();
const formulas = {
  price: () => 50,
  tip: (price) => price * 0.2,
  total: (price, tip) => price + tip,
  note: () => "thanks!",
};
const runs = [];
const flow = createDataflow({
  run(id, { deps }) {
    runs.push(id);
    const value = formulas[id](...deps.map((d) => values.get(d)));
    values.set(id, value);
    return value;
  },
});
flow.set("price").set("tip", { deps: ["price"] }).set("total", { deps: ["price", "tip"] }).set("note");
flow.runAll();
await flow.idle();
assert.equal(values.get("total"), 60);

runs.length = 0;
formulas.price = () => 80;
flow.invalidate("price");
await flow.idle();
assert.deepEqual(runs, ["price", "tip", "total"]);
assert.equal(values.get("total"), 96);
console.log("ok: total =", values.get("total"), "after recomputing", runs.join(" -> "));
