# dataflow

[![npm version](https://img.shields.io/npm/v/%40johnhenry%2Fdataflow.svg)](https://www.npmjs.com/package/@johnhenry/dataflow)
[![CI](https://github.com/johnhenry/dataflow/actions/workflows/ci.yml/badge.svg)](https://github.com/johnhenry/dataflow/actions/workflows/ci.yml)
[![license](https://img.shields.io/npm/l/%40johnhenry%2Fdataflow.svg)](LICENSE)

Full documentation: [opensource.johnhenry.me/dataflow](https://opensource.johnhenry.me/dataflow/)

An async dataflow scheduler. Nodes depend on other nodes; your async `run(id)` computes a node; dataflow decides **when** each node runs and remembers **how** its last run went. It never holds your values, so they can live wherever they need to: in a Map, inside a Web Worker, in a sandboxed iframe, on a server.

That split is the point. A spatial notebook, a spreadsheet or a build graph keeps values somewhere the scheduler can't (or shouldn't) reach, and still needs the hard parts done right: no glitches in a diamond, no slow stale run overwriting a newer one, errors that don't turn every downstream cell red, manual cells that know they're out of date.

Zero dependencies. ESM. Runs in browsers, Workers and Node.

## Contents

- [Install](#install)
- [Quick start](#quick-start)
- [States](#states)
- [Guarantees](#guarantees)
- [Skipping](#skipping)
- [Partial runs](#partial-runs)
- [Equality cut-off](#equality-cut-off)
- [Live nodes](#live-nodes)
- [API](#api)
- [Honest limitations](#honest-limitations)
- [Family](#family)
- [License](#license)

## Install

```sh
npm install @johnhenry/dataflow
```

**Provenance:** a new package, never published under another name. Under npm's caret rules `^0.0.0` matches only `0.0.0` (and `^0.0.1` only `0.0.1`), so pin exactly until a deliberate `0.1.0`; skip values need `0.0.1`, held runs and `UNCHANGED` `0.0.2`, live nodes `0.0.3`.

## Quick start

```js
import { createDataflow } from "@johnhenry/dataflow";

const values = new Map(); // yours, not the scheduler's
const formulas = {
  price: () => 50,
  tip: (price) => price * 0.2,
  total: (price, tip) => price + tip,
};

const flow = createDataflow({
  async run(id, { deps, signal }) {
    const value = await formulas[id](...deps.map((d) => values.get(d)));
    if (!signal.aborted) values.set(id, value); // a newer run superseded this one
    return value; // becomes state.result
  },
  onChange(id, state) {
    console.log(id, state.status);
  },
});

flow
  .set("price")
  .set("tip", { deps: ["price"] })
  .set("total", { deps: ["price", "tip"] });

flow.runAll();
await flow.idle();
values.get("total"); // 60

formulas.price = () => 80;
flow.invalidate("price"); // price, tip, total rerun -- in that order, once each
```

Runnable, self-verifying versions are in [`examples/`](examples/).

## States

| Status | Meaning |
| --- | --- |
| `idle` | Never run, or reset. |
| `waiting` | Asked to run, but an input is missing, errored, or hasn't run yet. Runs by itself once the input arrives. |
| `running` | `run` was called and hasn't settled. |
| `done` | The last run resolved. `state.result` holds what `run` returned. |
| `skipped` | The last run returned `SKIP` / `skip(reason)`, or an input was skipped and the skip spread here (see [Skipping](#skipping)). `state.skip` is `{ source, reason }`. |
| `error` | The last run threw or rejected (`state.error`), or the node is on a dependency cycle (`state.cycle === true`). |

A node queued behind upstream work keeps its previous state until it actually starts. Every state also carries `version` (settles), `runId` (started runs), `stale` (a manual node whose inputs or definition changed since its last run, or a dependent a [held run](#partial-runs) didn't rerun), `unchanged` (the last run returned [`UNCHANGED`](#equality-cut-off)) and `live` (the run is [live](#live-nodes): it settles more than once).

## Guarantees

- **Glitch-free.** A node never starts while anything upstream of it is queued or running. In a diamond (`a → b`, `a → c`, `b + c → d`), changing `a` runs `d` once, after both `b` and `c`.
- **Newest run wins.** Rerunning a node while a previous run is in flight aborts the previous run's `AbortSignal` and drops its result, even if it settles later. Write your side effects behind `if (!signal.aborted)`.
- **Errors don't cascade as errors.** When a node fails, its dependents become `waiting`, not `error`; only the node that broke shows an error, and the dependents rerun on their own once it's fixed.
- **Manual nodes stay put.** A node with `autorun: false` keeps its last result when inputs change, and is flagged `stale: true` until you `run` it.
- **Cycles are refused, not looped.** Every node on a cycle goes to `error` with `cycle: true` as soon as the edge is added, and comes back to `idle` when the cycle is broken.
- **Held runs stay held.** `run(id, { follow: false })` never starts a dependent that wasn't already asked to run; see [Partial runs](#partial-runs).
- **Skips spread, they don't stall.** A node that returns `SKIP` is `skipped`, and so is every dependent, without running, until one that declared `consumesSkip` decides what to do. Nothing changes for a graph that never skips.
- **Live sources stay glitch-free and paced.** A [live](#live-nodes) node between settles isn't busy, so a diamond below it sees one consistent version per item; an async iterable is pulled only after everything downstream of its previous item settled.

## Skipping

A node can decline to produce a value: its `run` returns `SKIP` (or `skip(reason)`). That is how a branch says "not this time": an if/else where only one side applies, a lookup that found nothing, a pane that's switched off. Inspired by [Rivet's control-flow-excluded values](https://github.com/Ironclad/rivet/blob/main/packages/docs/docs/user-guide/control-flow.md).

```js
import { createDataflow, SKIP, skip, isSkip } from "@johnhenry/dataflow";

const values = new Map();
const formulas = {
  amount: () => 40,
  small: (n) => (n < 100 ? `small: ${n}` : skip("not small")),
  large: (n) => (n >= 100 ? `large: ${n}` : SKIP),
  approval: (large) => `approve ${large}`,
  message: (...inputs) => inputs.find((v) => v !== undefined) ?? SKIP, // coalesce
};

const flow = createDataflow({
  async run(id, { deps, skipped, signal }) {
    const inputs = deps.map((d) => (skipped.includes(d) ? undefined : values.get(d)));
    const value = await formulas[id](...inputs);
    if (!signal.aborted) isSkip(value) ? values.delete(id) : values.set(id, value);
    return value;
  },
});

flow
  .set("amount")
  .set("small", { deps: ["amount"] })
  .set("large", { deps: ["amount"] })
  .set("approval", { deps: ["large"] }) // skipped with "large", never runs
  .set("message", { deps: ["small", "large"], consumesSkip: true }); // runs anyway

flow.runAll();
await flow.idle();
flow.get("approval"); // { status: "skipped", skip: { source: "large", reason: undefined }, ... }
values.get("message"); // "small: 40"
```

The rules:

- **A skip spreads.** When an input is `skipped`, a dependent becomes `skipped` too, without running, carrying the same `state.skip` (`source` is the node whose run returned the skip, `reason` what it passed to `skip`). In a diamond the join is decided once, after every branch settled.
- **Consumers run anyway.** `set(id, { deps, consumesSkip: true })` runs the node even when inputs were skipped; `ctx.skipped` lists them (it's `[]` for everyone else). `consumesSkip: ["a", "b"]` handles skips on those inputs only; a skip on any other input still spreads. A consumer whose inputs were all skipped can return `SKIP` itself, and that skip spreads on from it.
- **Coalesce** is a consumer that returns its first input not in `ctx.skipped` (or `SKIP` if there's none), as `message` above.
- **Race, if/else.** There is no first-to-finish race: a node never starts while anything upstream is running, so a consumer always sees every branch settled. Make the branches mutually exclusive (each skips unless its condition holds) and coalesce them; that is if/else, and it's what a race of exclusive branches computes.
- **Errors still win.** A node with an input that errored, is missing or hasn't run waits, as before, even if another input was skipped and even if it consumes skips. Fixing the error lets it resolve (run, or skip).
- **It recovers.** When the source produces a value again, the skipped dependents run normally (`state.skip` is gone).
- **Manual nodes stay put.** A done manual node whose input is skipped keeps its result and is flagged `stale`; running it then skips it without calling `run`. A skipped manual node whose input changes is flagged `stale` too, and reruns when asked.
- **Newest run wins**, for skips as for values: a superseded run's `SKIP` is dropped, and a skip spreading onto a running node aborts that run's signal and drops its result.
- **`version` counts runs.** A node whose own run returned `SKIP` settled a run (`version` +1); a node skipped because an input was skipped didn't run (`version` unchanged, `runId` +1).
- **Your values are yours.** The scheduler can't clear the value you stored for a node before it was skipped; drop it when `run` returns a skip (as above) or when `onChange` reports `skipped`.

## Partial runs

A notebook with expensive cells (an LLM call, a paged fetch, an audio render) wants finer runs than "this cell and everything that follows": run one cell and look at it before paying for the cells below, or run a cell together with whatever it needs.

```js
flow.run("prompt", { follow: false }); // only "prompt"; "answer" below it is flagged stale
flow.runAll({ ids: ["topic", "prompt"], follow: false }); // that set, in order; everything else held
flow.runAll({ ids: ["prompt", "answer", "shown"] }); // a one-shot "run from here", manual cells included
```

- **`follow: false` holds the run's dependents.** For that run, every dependent is treated as if it were a manual node: a `done` or `skipped` one keeps its result and is flagged `stale` (it reruns when you `run` it, or when an input changes again and it follows normally); nothing below it is touched, the same as for a manual node. `follow` defaults to `true`, which is the 0.0.1 behavior.
- **`runAll({ ids, follow: false })`** runs exactly the nodes in `ids` (in dependency order, each once) and holds every dependent outside the set. Planning the set ("this node and its stale upstream") is the caller's: `deps(id)`, `dependents(id)`, `get(id)` and `order(ids)` have what it needs.
- **What a hold doesn't stop.** A dependent that is `waiting` (its input had failed) was already asked to run, and runs when the input recovers, as a waiting manual node does. A dependent that is queued anyway (in the same `runAll`, or by another `run`) runs. A held run that fails makes its dependents wait, as any failure does.
- **The request stands until the node starts.** A node asked to run held while its input is broken waits, and when it finally runs, its dependents are held. The latest request for a queued node wins (`run(id)` after `run(id, { follow: false })` follows); `reset()` forgets it.
- **Skips are held too.** A held run that returns `SKIP` flags its done dependents `stale` instead of spreading the skip to them.

## Equality cut-off

The scheduler doesn't hold values, so it can't tell that a new value equals the old one; you can. Return `UNCHANGED` from `run` and the node settles `done` without rerunning what depends on it:

```js
import { createDataflow, UNCHANGED } from "@johnhenry/dataflow";

const values = new Map();
const flow = createDataflow({
  async run(id, { deps, signal }) {
    const value = await formulas[id](...deps.map((d) => values.get(d)));
    if (signal.aborted) return value;
    if (values.has(id) && Object.is(values.get(id), value)) return UNCHANGED; // your equality
    values.set(id, value);
    return value;
  },
});
```

- **What it does.** The node settles `done` with `unchanged: true`. `version` doesn't bump (no new value) and `runId` does (it was a run). `state.result` keeps the previous result; return `unchanged(result)` to replace it (a fresh preview of an equal value, say).
- **Dependents that already ran on the value are left alone:** autorun ones don't rerun, manual ones aren't flagged `stale`, and errored ones aren't retried. Dependents that are `waiting`, or that never ran (an autorun node added after the input last changed), still run: they don't have a result for this value yet. A dependent queued for its own reasons still runs.
- **Only after a `done`.** If the node's last settled run wasn't `done` (first run, an error, a skip, a cycle, a `reset()`), there is nothing to compare with: `UNCHANGED` counts as a new value (`result` is `undefined`, or what `unchanged(result)` carried) and propagates.
- **Newest run wins** for it as for any result: a superseded run's `UNCHANGED` is dropped.
- It is `Symbol.for("@johnhenry/dataflow.unchanged")`; like `SKIP` it can't cross `postMessage`, so a Worker returns its own marker and your `run` maps it.

## Live nodes

Some values change by themselves: a clock, a socket, a sensor, a file watch, a model streaming tokens. Rerunning the producer for each new value is wrong (it would reconnect every time); a **live run** settles more than once instead. Each settle is a new value of the node: `version` +1, `state.result` updated, dependents run as for any settle. Nothing changes for a graph that doesn't use it.

```js
const flow = createDataflow({
  async *run(id, ctx) {
    // `run` may be an async generator itself, or return any async iterable.
    const socket = connect(feeds[id], { signal: ctx.signal }); // stops iterating when the signal aborts
    try {
      for await (const message of socket) yield message; // each item settles the node
    } finally {
      socket.close(); // runs when the run is superseded, reset or deleted, or the feed ends
    }
  },
});
```

A run goes live in either of two ways, or both:

- **Return an async iterable** (pull form). Each item settles the node: a value settles `done`, `SKIP` / `skip(reason)` settles `skipped` (and spreads), `UNCHANGED` / `unchanged(result)` works as it does when returned. A thrown error settles `error` and ends the run. When the iterable finishes the run ends; one that finishes without an item settles `done` with `result: undefined`, like a function that returns nothing. Its `{ done: true }` value is ignored, as `for await` ignores it.
- **Call `ctx.emit(result)`** (push form, for callback sources). It settles the node right away, exactly like an item, and returns `true`; it returns `false` and does nothing once the run was superseded, reset, deleted, closed or ended. The run stays live until its promise resolves, `ctx.close()` is called, or it errors. Once a run has emitted, its resolved value no longer settles anything: resolving just ends it (emit the last value before returning). A run may emit and then return an async iterable, which takes over.

```js
async run(id, { emit, signal }) {
  const source = new EventSource(urls[id]);
  source.onmessage = (event) => emit(event.data);
  await new Promise((resolve) => signal.addEventListener("abort", resolve)); // stay live
  source.close();
}
```

**`state.live`** is `true` while the run is open, in `get(id)` and in every `onChange`. The status keeps its meaning: `running` until the first settle (dependents wait for it, as for any run), then the status of the last settle. `runId` stays the same across a live run's settles; `version` counts them.

### Backpressure

An async iterable is pulled for its next item only once **everything downstream of its previous item has settled**: no node downstream of it (transitively, not just its direct dependents) is queued to run or `running`. It is the glitch-freedom rule (`upstreamBusy`) applied downstream, and it means a burst from a fast producer reaches the graph one item at a time, each one through the whole graph, with no run superseded and nothing buffered in the scheduler (the producer waits at its `yield`). Precisely, a downstream node counts as settled when it is:

- `done`, `skipped`, `error`, `waiting` or `idle` and not queued. A `waiting` dependent (an input failed) and an errored one don't hold the producer.
- **Manual** (`autorun: false`): it is flagged `stale`, not queued, so it never holds the producer.
- **Held:** a [held](#partial-runs) live run (`run(id, { follow: false })`) flags its dependents `stale` on every settle instead of running them, so nothing holds it: it is pulled as fast as it yields.
- **Live itself:** a live dependent between settles isn't busy. One that hasn't settled yet in its current run is `running`, so it holds the producer until its first settle. (A live `resume` dependent that waits for a change before its first settle would therefore wait forever: settle once first.)
- **A `resume` dependent** counts as settled once it has been told about the change (see below): what it does with the notice is its own business, and its notices coalesce.

The first item is pulled as soon as the iterable is returned (it is the run's own result). While the graph is `pause()`d no item is pulled; emits still settle, as running runs still do. `ctx.emit` has no backpressure: each emit settles at once, and dependents still running on an older emit are superseded (newest wins), so a burst of emits coalesces downstream to the latest.

### Glitch-freedom

A live node between settles isn't busy, so it doesn't block its dependents; each settle starts a normal wave. In a diamond (`live → b`, `live → c`, `b + c → d`) with a live source, `d` runs once per item, after both `b` and `c` saw that item: one consistent version per settle. With `emit`, a newer value supersedes the wave still in flight, and the join still never mixes versions.

### Ending a live run: supersede, reset, delete

A newer run of the node (an explicit `run`, or an input change, which restarts it by default), `reset()`, `delete(id)`, an input that fails (the node waits) or a skip spreading onto it all end the live run: its `AbortSignal` aborts **and** its iterable's `return()` is called, so an async generator's `finally` runs. Items and emits that arrive later are dropped (`emit` returns `false`). An iterable returned by a run that was already superseded is closed unread.

`return()` reaches a generator only at a `yield`. One that is held by backpressure is at a `yield`, so its `finally` runs at once; one that is awaiting something (the next socket message) runs it when that await ends, so make long waits follow `ctx.signal` (as `readings()` does in [`examples/06-live-nodes.mjs`](examples/06-live-nodes.mjs), or with `abortable()` from `@johnhenry/iteration`). `ctx.close()` ends the run from inside: later emits are refused and an open iterable is closed, but the signal doesn't abort (it is an end, not a cancellation). Errors thrown by a producer's cleanup are dropped, like the rejection of any superseded run.

### Resume instead of restart

By default an input change restarts a live run, like any run: the old one is closed and `run` is called again. A node set with **`resume: true`** keeps its live run instead, and the run is told about the change:

```js
flow.set("total", { deps: ["amount"], resume: true });

async *run(id, ctx) {
  let total = values.get("amount");
  yield total;
  for await (const change of ctx.changes) {
    // change: { deps, skipped, changed: ["amount"] }
    total += values.get("amount");
    yield total; // a running total no restart could keep
  }
}
```

- **`ctx.changes`** is an async iterable of change notices `{ deps, skipped, changed }`: the node's dependencies now, the skipped inputs (for a `consumesSkip` node), and the inputs that settled since the last notice. It is a mailbox of one, **coalesced, latest wins**: notices that arrive before the run takes the previous one merge into one (latest `deps` and `skipped`, `changed` merged), and they are kept from the moment the run starts, so a generator that starts listening after its first `yield` misses nothing. It is meant for one consumer, and it ends when the run ends (a `for await` over it then simply finishes). For a node without `resume` it never yields.
- **`ctx.onChange(fn)`** is the callback form: `fn(notice)` for every notice (not coalesced), returning an unsubscribe function; a throwing callback is reported like a throwing listener. It suits the `emit` form: `ctx.onChange(() => emit(recompute()))`.
- Notices follow the same rules as runs: one per upstream wave, only once nothing upstream is busy (glitch-free), after the change actually reaches the node (a manual node is flagged `stale`, a held one too; an `UNCHANGED` input doesn't notify).
- **What still restarts it:** a run that isn't live yet (it hasn't emitted or returned an iterable), an explicit `run(id)` / `runAll(...)`, and `invalidate(id)` (a new definition). **What ends it:** an input that fails (the node waits) or a skip spreading onto it, as for any node.

**Why a node option.** Restart or resume is a property of the node (the caller knows, per cell, whether its code is written to resume), and the scheduler has to know it before the change arrives, to decide between superseding the run and notifying it; a flag the handler sets would make that decision depend on how far the run had got. As an option it sits with `autorun` and `consumesSkip`, can be read back (`resumes(id)`; `resume()` already un-pauses the graph), and changes with `set` like them. `ctx.changes` and `ctx.onChange` are on every run, so the handler needs no flag of its own.

The scheduler still stores and compares no values: a notice says which inputs changed, not what to; the run reads them wherever they live.

## API

### `createDataflow({ run, onChange?, schedule? })`

- `run(id, { runId, signal, deps, skipped, emit, close, changes, onChange })` -- compute one node; may return a value or a promise. `deps` is the dependency list at the moment the run started; `skipped` lists the inputs that were skipped (only ever non-empty for a node with `consumesSkip`). Return `SKIP` or `skip(reason)` to skip the node, and `UNCHANGED` or `unchanged(result)` when the value equals the previous one. Return an async iterable, or call `emit(result)`, for a [live run](#live-nodes); `close()` ends it; `changes` / `onChange(fn)` deliver change notices to a live `resume` node.
- `onChange(id, state)` -- called on every state change (also see `subscribe`).
- `schedule(fn)` -- how flushes are deferred; default `queueMicrotask`.

Returns a graph object; mutating methods return it, so calls chain.

| Method | Does |
| --- | --- |
| `set(id, { deps = [], autorun = true, consumesSkip = false, resume = false })` | Add a node, or replace its dependencies / autorun flag / skip handling (`true`, or the input ids it consumes skips from) / [resume](#resume-instead-of-restart) flag. Never runs anything by itself. |
| `delete(id)` | Remove a node; its dependents start waiting. |
| `run(id, { follow = true })` | Run now, whatever the autorun flag. Autorun dependents follow; `follow: false` holds them (flagged `stale`, not run). |
| `invalidate(id)` | The definition changed: autorun nodes rerun, manual nodes become `stale`. |
| `runAll({ all = true, ids, follow = true })` | Queue nodes in dependency order; `all: false` skips manual nodes; `follow: false` holds dependents outside the set. |
| `reset()` | Forget every result, abort in-flight runs and close live ones (for example after the place your values live was restarted). |
| `pause()` / `resume()` / `paused` | Hold new runs and live pulls; running ones still settle (and emit). |
| `get(id)`, `has(id)`, `deps(id)`, `autorun(id)`, `consumesSkip(id)`, `resumes(id)`, `dependents(id)`, `ids()` | Read the graph. `resumes(id)` is the `resume` option. |
| `order(ids?)` | Topological order, dependencies first; nodes on a cycle are omitted. |
| `subscribe(fn)` | Listen to every change; returns an unsubscribe function. A listener that throws is reported (`reportError`) and affects neither other listeners nor scheduling, like an `EventTarget` listener. |
| `idle()` | Resolves once nothing is queued or running (or the graph is paused). A live node between settles isn't running, so `idle()` doesn't wait for an open stream; one that hasn't settled yet in its run is, and it does. |

### `SKIP`, `skip(reason?)`, `isSkip(value)`

- `SKIP` -- return it from `run` to skip the node. It is `Symbol.for("@johnhenry/dataflow.skip")`, so two copies of the package agree; it can't cross `postMessage`, so a Worker returns its own marker and your `run` maps it to `SKIP`.
- `skip(reason)` -- a frozen skip that carries `reason` to `state.skip.reason` of the node and everything it spreads to.
- `isSkip(value)` -- true for both.

### `UNCHANGED`, `unchanged(result?)`, `isUnchanged(value)`

- `UNCHANGED` -- return it from `run` when the new value equals the previous one; see [Equality cut-off](#equality-cut-off).
- `unchanged(result)` -- the same, and `result` replaces `state.result`.
- `isUnchanged(value)` -- true for both.

TypeScript declarations ship in `src/index.d.ts`.

## Honest limitations

- **It doesn't store or compare values.** That is deliberate, so deciding that a value didn't change (equality cut-off) is yours: compare in `run` and return [`UNCHANGED`](#equality-cut-off); the scheduler only acts on the answer.
- **It doesn't plan partial runs.** `follow: false` holds dependents; which nodes a "run up to here" should include (stale upstream, never-run inputs) is the caller's choice, made from `deps`, `get` and `order`.
- **Cancellation is cooperative.** A superseded run's result is dropped and its signal aborts, but the scheduler can't stop code that ignores the signal. If you need a hard kill, run in something you can terminate (a Worker) and call `reset()` after restarting it.
- **No first-to-finish race.** Glitch-freedom means a node with skip-consuming inputs runs once all of them settled, never on the first one; see [Skipping](#skipping).
- **Live producers must cooperate.** A producer that yields forever without ever awaiting anything that takes a turn of the event loop (a timer, I/O) starves it, as any endless async loop does; and `return()` reaches an async generator only at a `yield`, so a long await should follow `ctx.signal` for cleanup to be prompt. A shared source that two runs read in turn (a queue) can hand an item to a run that is about to be superseded; the scheduler drops that run's late items, it can't give them to the next run.
- **Async iterables are always live.** A `run` that returns (or resolves to) an object with `Symbol.asyncIterator` -- an async generator, a `ReadableStream` -- makes the run live. To keep such an object as a plain result, wrap it (`return { stream }`).
- **Scans are linear.** Finding dependents walks every node; that is fine for hundreds or a few thousand nodes (a notebook, a spreadsheet tab), not for a million-cell sheet.
- **One process.** The graph is in-memory and not shared across tabs or machines.

## Family

dataflow is the scheduling half of a notebook-shaped stack: it decides when cells run, while other packages hold and show the values.

- **[`@johnhenry/andbox`](https://github.com/johnhenry/andbox)** -- where the values can live. A `run` that calls `sandbox.evaluate(...)` keeps every cell's value inside andbox's Worker (functions and class instances included) while dataflow tracks states on the page; when a timeout hard-kills the Worker, call `reset()` and `runAll()`.
- **[`@johnhenry/inspectable`](https://github.com/johnhenry/inspectable)** -- what `run` returns. Return `inspectable`'s `serialize(value)` from inside the sandbox and `state.result` is a structured-clone-safe preview a `<value-inspector>` can render.
- **[`@johnhenry/signalle`](https://github.com/johnhenry/signalle)** -- signals for UI state. signalle's `computed` is the right tool when the values live in the same realm and you want them as signals; dataflow is for when they don't, or when you need cancellation, error/waiting states and manual cells.
- **[`@johnhenry/math`](https://github.com/johnhenry/math)** -- its `CellGraph` is a synchronous reactive graph of math cells that stores values itself; dataflow is the async, value-agnostic counterpart.

## License

MIT
