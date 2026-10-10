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
- [API](#api)
- [Honest limitations](#honest-limitations)
- [Family](#family)
- [License](#license)

## Install

```sh
npm install @johnhenry/dataflow
```

**Provenance:** a new package, never published under another name. Under npm's caret rules `^0.0.0` matches only `0.0.0` (and `^0.0.1` only `0.0.1`), so pin exactly until a deliberate `0.1.0`; skip values need `0.0.1`.

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

A node queued behind upstream work keeps its previous state until it actually starts. Every state also carries `version` (settled runs), `runId` (started runs), `stale` (a manual node whose inputs or definition changed since its last run, or a dependent a [held run](#partial-runs) didn't rerun) and `unchanged` (the last run returned [`UNCHANGED`](#equality-cut-off)).

## Guarantees

- **Glitch-free.** A node never starts while anything upstream of it is queued or running. In a diamond (`a → b`, `a → c`, `b + c → d`), changing `a` runs `d` once, after both `b` and `c`.
- **Newest run wins.** Rerunning a node while a previous run is in flight aborts the previous run's `AbortSignal` and drops its result, even if it settles later. Write your side effects behind `if (!signal.aborted)`.
- **Errors don't cascade as errors.** When a node fails, its dependents become `waiting`, not `error`; only the node that broke shows an error, and the dependents rerun on their own once it's fixed.
- **Manual nodes stay put.** A node with `autorun: false` keeps its last result when inputs change, and is flagged `stale: true` until you `run` it.
- **Cycles are refused, not looped.** Every node on a cycle goes to `error` with `cycle: true` as soon as the edge is added, and comes back to `idle` when the cycle is broken.
- **Held runs stay held.** `run(id, { follow: false })` never starts a dependent that wasn't already asked to run; see [Partial runs](#partial-runs).
- **Skips spread, they don't stall.** A node that returns `SKIP` is `skipped`, and so is every dependent, without running, until one that declared `consumesSkip` decides what to do. Nothing changes for a graph that never skips.

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

## API

### `createDataflow({ run, onChange?, schedule? })`

- `run(id, { runId, signal, deps, skipped })` -- compute one node; may return a value or a promise. `deps` is the dependency list at the moment the run started; `skipped` lists the inputs that were skipped (only ever non-empty for a node with `consumesSkip`). Return `SKIP` or `skip(reason)` to skip the node, and `UNCHANGED` or `unchanged(result)` when the value equals the previous one.
- `onChange(id, state)` -- called on every state change (also see `subscribe`).
- `schedule(fn)` -- how flushes are deferred; default `queueMicrotask`.

Returns a graph object; mutating methods return it, so calls chain.

| Method | Does |
| --- | --- |
| `set(id, { deps = [], autorun = true, consumesSkip = false })` | Add a node, or replace its dependencies / autorun flag / skip handling (`true`, or the input ids it consumes skips from). Never runs anything by itself. |
| `delete(id)` | Remove a node; its dependents start waiting. |
| `run(id, { follow = true })` | Run now, whatever the autorun flag. Autorun dependents follow; `follow: false` holds them (flagged `stale`, not run). |
| `invalidate(id)` | The definition changed: autorun nodes rerun, manual nodes become `stale`. |
| `runAll({ all = true, ids, follow = true })` | Queue nodes in dependency order; `all: false` skips manual nodes; `follow: false` holds dependents outside the set. |
| `reset()` | Forget every result, abort in-flight runs (for example after the place your values live was restarted). |
| `pause()` / `resume()` / `paused` | Hold new runs; running ones still settle. |
| `get(id)`, `has(id)`, `deps(id)`, `autorun(id)`, `consumesSkip(id)`, `dependents(id)`, `ids()` | Read the graph. |
| `order(ids?)` | Topological order, dependencies first; nodes on a cycle are omitted. |
| `subscribe(fn)` | Listen to every change; returns an unsubscribe function. A listener that throws is reported (`reportError`) and affects neither other listeners nor scheduling, like an `EventTarget` listener. |
| `idle()` | Resolves once nothing is queued or running (or the graph is paused). |

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
