// Events: clicks that must each be handled once and in order, by a node that
// reads values without being run by them.
//
// 1. `sample`: the "send" node reads the draft but isn't triggered by it, so
//    typing doesn't send anything (RxJS's withLatestFrom, Noodl's "Run" input).
// 2. `queue: true`: clicks that arrive while a slow send is in flight queue
//    up behind it. Each one is sent once, in order, and none is aborted.
//    `ctx.trigger` says which click a run is for. A superseding node behind
//    the same button only finishes for the last click.
// 3. `queueLimit`: a bounded queue drops its oldest triggers and counts them
//    in `state.dropped`.
import assert from "node:assert/strict";
import { createDataflow, SKIP } from "@johnhenry/dataflow";

const network = () => new Promise((resolve) => setImmediate(resolve)); // a stand-in for a slow request
const values = new Map(); // yours, not the scheduler's
let draft = "hello";
let click; // the button's emit: each call is one click event
const sent = [];
const latest = [];
const audited = [];

const flow = createDataflow({
  async run(id, ctx) {
    switch (id) {
      case "button":
        click = ctx.emit;
        // No click yet. Without this, a source that hasn't emitted is
        // `running`: its dependents, and `idle()`, would wait for a first click.
        ctx.emit(SKIP);
        return new Promise((resolve) => ctx.signal.addEventListener("abort", resolve)); // stays live
      case "draft":
        values.set("draft", draft);
        return draft;
      case "send": {
        const n = ctx.trigger.result; // which click this run is for
        const text = values.get("draft"); // sampled: read, never a trigger
        await network();
        await network();
        if (ctx.signal.aborted) return;
        sent.push(`#${n}: ${text}`);
        return n;
      }
      case "latest": // the same button, without `queue`: newest wins
        await network();
        if (!ctx.signal.aborted) latest.push(flow.get("button").result);
        return;
      case "audit": // a bounded queue
        await network();
        audited.push(ctx.trigger.result);
        return;
    }
  },
});

flow
  .set("button")
  .set("draft")
  .set("send", { deps: ["button"], sample: ["draft"], queue: true })
  .set("latest", { deps: ["button"] })
  .set("audit", { deps: ["button"], queue: true, queueLimit: 2 });
flow.run("button").run("draft"); // (runAll would also queue a `run` trigger on each queue node)
await flow.idle();
assert.equal(flow.get("send").status, "skipped", "no click yet");

// 1. Typing doesn't send: the draft is sampled.
draft = "hello world";
flow.invalidate("draft");
await flow.idle();
assert.deepEqual(sent, []);
assert.equal(flow.get("send").version, 0, "send never ran");

// 2. A burst of clicks while the first send is in flight.
click(1);
await network(); // send #1 is in flight
click(2);
click(3);
click(4);
click(5);
assert.equal(flow.get("send").queued, 4, "clicks 2 to 5 wait behind it");
await flow.idle();
assert.deepEqual(sent, ["#1: hello world", "#2: hello world", "#3: hello world", "#4: hello world", "#5: hello world"]);
assert.deepEqual(latest, [5], "the superseding node finished only for the last click");

// 3. The bounded queue kept the run in flight and its last two triggers.
assert.deepEqual(audited, [1, 4, 5]);
assert.equal(flow.get("audit").dropped, 2, "clicks 2 and 3 were dropped, oldest first");

// A new draft is read by the next click, not sent by itself.
draft = "bye";
flow.invalidate("draft");
click(6);
await flow.idle();
assert.equal(sent.at(-1), "#6: bye");

flow.reset(); // clears every queue and ends the button's live run
console.log("ok: typing doesn't send, every click is sent once and in order, a bounded queue counts what it dropped");
