# dataflow

[![npm version](https://img.shields.io/npm/v/%40johnhenry%2Fdataflow.svg)](https://www.npmjs.com/package/@johnhenry/dataflow)
[![CI](https://github.com/johnhenry/dataflow/actions/workflows/ci.yml/badge.svg)](https://github.com/johnhenry/dataflow/actions/workflows/ci.yml)
[![license](https://img.shields.io/npm/l/%40johnhenry%2Fdataflow.svg)](LICENSE)

An async dataflow scheduler. Nodes depend on other nodes; your async `run(id)` computes a node; dataflow decides **when** each node runs and remembers **how** its last run went. It never holds your values, so they can live wherever they need to: in a Map, inside a Web Worker, in a sandboxed iframe, on a server.

That split is the point. A spatial notebook, a spreadsheet or a build graph keeps values somewhere the scheduler can't (or shouldn't) reach, and still needs the hard parts done right: no glitches in a diamond, no slow stale run overwriting a newer one, errors that don't turn every downstream cell red, manual cells that know they're out of date.

Zero dependencies. ESM. Runs in browsers, Workers and Node.

## Contents

- [Install](#install)
- [Quick start](#quick-start)
- [States](#states)
- [Guarantees](#guarantees)
- [API](#api)
- [Honest limitations](#honest-limitations)
- [Family](#family)
- [License](#license)

## Install

```sh
npm install @johnhenry/dataflow
```

**Provenance:** a new package, never published under another name. Under npm's caret rules `^0.0.0` matches only `0.0.0`, so pin exactly until a deliberate `0.1.0`.

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
| `error` | The last run threw or rejected (`state.error`), or the node is on a dependency cycle (`state.cycle === true`). |

A node queued behind upstream work keeps its previous state until it actually starts. Every state also carries `version` (settled runs), `runId` (started runs) and `stale` (a manual node whose inputs or definition changed since its last run).

## Guarantees

- **Glitch-free.** A node never starts while anything upstream of it is queued or running. In a diamond (`a → b`, `a → c`, `b + c → d`), changing `a` runs `d` once, after both `b` and `c`.
- **Newest run wins.** Rerunning a node while a previous run is in flight aborts the previous run's `AbortSignal` and drops its result, even if it settles later. Write your side effects behind `if (!signal.aborted)`.
- **Errors don't cascade as errors.** When a node fails, its dependents become `waiting`, not `error`; only the node that broke shows an error, and the dependents rerun on their own once it's fixed.
- **Manual nodes stay put.** A node with `autorun: false` keeps its last result when inputs change, and is flagged `stale: true` until you `run` it.
- **Cycles are refused, not looped.** Every node on a cycle goes to `error` with `cycle: true` as soon as the edge is added, and comes back to `idle` when the cycle is broken.

## API

### `createDataflow({ run, onChange?, schedule? })`

- `run(id, { runId, signal, deps })` -- compute one node; may return a value or a promise. `deps` is the dependency list at the moment the run started.
- `onChange(id, state)` -- called on every state change (also see `subscribe`).
- `schedule(fn)` -- how flushes are deferred; default `queueMicrotask`.

Returns a graph object; mutating methods return it, so calls chain.

| Method | Does |
| --- | --- |
| `set(id, { deps = [], autorun = true })` | Add a node, or replace its dependencies / autorun flag. Never runs anything by itself. |
| `delete(id)` | Remove a node; its dependents start waiting. |
| `run(id)` | Run now, whatever the autorun flag. Autorun dependents follow. |
| `invalidate(id)` | The definition changed: autorun nodes rerun, manual nodes become `stale`. |
| `runAll({ all = true, ids })` | Queue nodes in dependency order; `all: false` skips manual nodes. |
| `reset()` | Forget every result, abort in-flight runs (for example after the place your values live was restarted). |
| `pause()` / `resume()` / `paused` | Hold new runs; running ones still settle. |
| `get(id)`, `has(id)`, `deps(id)`, `autorun(id)`, `dependents(id)`, `ids()` | Read the graph. |
| `order(ids?)` | Topological order, dependencies first; nodes on a cycle are omitted. |
| `subscribe(fn)` | Listen to every change; returns an unsubscribe function. A listener that throws is reported (`reportError`) and affects neither other listeners nor scheduling, like an `EventTarget` listener. |
| `idle()` | Resolves once nothing is queued or running (or the graph is paused). |

TypeScript declarations ship in `src/index.d.ts`.

## Honest limitations

- **It doesn't store values.** That is deliberate, but it means "rerun only if the value actually changed" (equality cut-off) is yours: return early from `run` and compare in `onChange` if you need it.
- **Cancellation is cooperative.** A superseded run's result is dropped and its signal aborts, but the scheduler can't stop code that ignores the signal. If you need a hard kill, run in something you can terminate (a Worker) and call `reset()` after restarting it.
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
