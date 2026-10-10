// An error upstream leaves dependents "waiting" (their input is unavailable),
// not "error" -- so only the pane that actually broke shows an error.
import assert from "node:assert/strict";
import { createDataflow } from "@johnhenry/dataflow";

let broken = true;
const flow = createDataflow({
  run(id) {
    if (id === "fetch" && broken) throw new Error("network down");
    return id;
  },
});
flow.set("fetch").set("parse", { deps: ["fetch"] }).set("chart", { deps: ["parse"] });
flow.runAll();
await flow.idle();
assert.equal(flow.get("fetch").status, "error");
assert.equal(flow.get("parse").status, "waiting");
assert.equal(flow.get("chart").status, "waiting");

broken = false;
flow.invalidate("fetch");
await flow.idle();
assert.deepEqual(["fetch", "parse", "chart"].map((id) => flow.get(id).status), ["done", "done", "done"]);
console.log("ok: recovered after the error was fixed");
