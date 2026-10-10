// A node can decline to produce a value by returning SKIP (or skip(reason)).
// The skip spreads: its dependents become "skipped" without running, instead
// of erroring or waiting forever. A node that declares `consumesSkip` runs
// anyway and sees which inputs were skipped, so it can take whichever branch
// produced a value (a coalesce).
import assert from "node:assert/strict";
import { createDataflow, SKIP, skip, isSkip } from "@johnhenry/dataflow";

const values = new Map(); // yours, not the scheduler's
const formulas = {
  amount: () => 120,
  // Two mutually exclusive branches: exactly one produces a value.
  small: (amount) => (amount < 100 ? `small order: ${amount}` : skip("not small")),
  large: (amount) => (amount >= 100 ? `large order: ${amount}, needs approval` : SKIP),
  approval: (large) => `approval requested for "${large}"`,
  // A coalesce: the first input that wasn't skipped.
  message: (...inputs) => inputs.find((value) => value !== undefined) ?? SKIP,
};
const ran = [];

const flow = createDataflow({
  async run(id, { deps, skipped, signal }) {
    ran.push(id);
    // A skipped input has no value: pass undefined for it.
    const inputs = deps.map((d) => (skipped.includes(d) ? undefined : values.get(d)));
    const value = await formulas[id](...inputs);
    if (!signal.aborted) {
      if (isSkip(value)) values.delete(id);
      else values.set(id, value);
    }
    return value;
  },
});

flow
  .set("amount")
  .set("small", { deps: ["amount"] })
  .set("large", { deps: ["amount"] })
  .set("approval", { deps: ["large"] })
  .set("message", { deps: ["small", "large"], consumesSkip: true });

flow.runAll();
await flow.idle();
assert.equal(flow.get("small").status, "skipped");
assert.deepEqual(flow.get("small").skip, { source: "small", reason: "not small" });
assert.equal(values.get("message"), "large order: 120, needs approval");
assert.equal(flow.get("approval").status, "done");

// The amount drops below 100: the large branch skips, and the skip spreads to
// "approval" without running it. "message" consumes the skip and switches branch.
formulas.amount = () => 40;
ran.length = 0;
flow.invalidate("amount");
await flow.idle();
assert.equal(flow.get("large").status, "skipped");
assert.equal(flow.get("approval").status, "skipped");
assert.equal(flow.get("approval").skip.source, "large");
assert.ok(!ran.includes("approval"), "a skipped dependent never runs");
assert.equal(values.get("message"), "small order: 40");

// Back above 100: the skipped chain recovers and runs normally.
formulas.amount = () => 300;
flow.invalidate("amount");
await flow.idle();
assert.equal(flow.get("approval").status, "done");
assert.equal(values.get("approval"), 'approval requested for "large order: 300, needs approval"');
console.log("ok: skipped branches don't run, the coalesce takes the one that did");
