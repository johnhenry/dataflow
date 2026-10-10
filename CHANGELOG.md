# Changelog

## 0.0.4 (2026-10-10)

Events: sampled (non-triggering) inputs and queued (non-superseding) nodes (#8, this PR; for johnhenry/miso#99, designed in johnhenry/miso#94). Additive and opt-in: a graph that uses neither `sample` nor `queue` produces exactly 0.0.3's `onChange` sequence (no new state keys; checked by a differential fuzz against 0.0.3 over 800 random graphs that also use live runs, emits and resume); every run's `ctx` gains `sampled` (`[]`).

- **`set(id, { sample: [ids] })`: inputs that are read but don't trigger** (`withLatestFrom`; Noodl's Function with Run connected). A sampled input orders and gates the node like a dependency: the node never starts while it, or anything upstream of it, is queued or running (glitch-free, including a sampled diamond), waits until it has settled once, and waits at its next trigger while it is waiting or failing (and runs once it settles again). Its settles never run, stale, invalidate or notify the node, or queue a trigger. A skipped sampled input spreads its skip when the node is triggered, unless consumed (`consumesSkip`). `ctx.deps` lists `deps` then the sampled ids; **`ctx.sampled`** names them. A cycle through a sampled edge is a cycle; an id in both lists triggers.
- **`set(id, { queue: true, queueLimit })`: triggers queue instead of superseding.** Each trigger (a settle of a dependency in `deps`, or an explicit `run` / `runAll` / `invalidate`) gets its own run, one after another, in order; none is aborted by a newer trigger. **`ctx.trigger`** (frozen) says what a run is for: `{ type: "settle", id, status, version, result, skip? }` (the dependency's result reference) or `{ type: "run" }` / `{ type: "invalidate" }`. Each trigger is judged by its own settle (a value queued before a skip still runs; a skip is answered with a skip) and keeps its own `follow`.
- **Bound and counts.** `queueLimit` (default 1000, or `Infinity`) caps waiting triggers; past it the oldest is dropped. Queue nodes, and only they, carry **`state.queued`** and **`state.dropped`**.
- **Clearing.** `reset()`, `delete(id)`, and the new **`run(id, { replace: true })`** (drop the queue and supersede the run in flight) clear the queue. A failing input aborts the run in flight and puts its trigger back at the head, so it runs again once the input recovers.
- **Live sources.** A live source feeding a queue node delivers every value in order: emitted bursts queue, and pulled items wait for the queue (backpressure counts a queue node with triggers left as busy). A queue node's own live run holds its queue until it ends.
- **`queue` and `resume` can't be combined** (`TypeError`); a `resume` node upstream of a queue node works, each of its settles a trigger.
- New readers `samples(id)`, `queues(id)`, `queueLimit(id)`; `dependents(id)` keeps listing only the nodes `id` triggers.
- Types: `NodeOptions.sample` / `queue` / `queueLimit`, `NodeState.queued` / `dropped`, `RunContext.sampled` / `trigger`, `Trigger<R>`, `RunNodeOptions` (`replace`), and the new readers.
- New example `examples/07-events-sample-and-queue.mjs`; README section "Events: sampled inputs and queued nodes"; Guarantees, Backpressure, API and Honest limitations updated (queues hold triggers, not values; one run per triggering settle, so once per branch of a diamond; an interrupted queued run is retried; an event source that hasn't fired yet is `running`, so emit `SKIP` first).
- **Upgrading:** under npm's caret rules `^0.0.3` matches only `0.0.3`; change the range to `^0.0.4` (or `0.0.4`).

## 0.0.3 (2026-10-10)

Live nodes: runs that settle more than once, with an opt-in resume (#6, #7; designed in johnhenry/miso#94). Additive and opt-in: a graph whose runs never return `live(...)`, aren't an `async function*`, and never call `ctx.emit` produces the same `onChange` sequence as 0.0.2 (every state now also carries `live: false`); an async iterable returned as a plain value (a `ReadableStream`, a generator object) is still a plain value.

- **`live(iterable)`, `isLive(value)`, `LIVE`: a `run` that returns `live(iterable)` is live**, and so is every run of a `run` that is itself an `async function*` (recognized by its built-in type tag; a function that merely returns a generator needs the marker). Each item settles the node (`version` +1, `state.result`, dependents run); `SKIP` / `skip(reason)` items settle `skipped`, `UNCHANGED` / `unchanged(result)` items work as when returned, a thrown error settles `error` and ends the run.
- **`ctx.emit(result)`** settles the node from inside a run (push form, no backpressure) and returns `false` once the run is superseded, reset, deleted, closed or ended. The run stays live until its promise resolves (its resolved value then doesn't settle again) or **`ctx.close()`**.
- **`state.live`** (new, `false` by default) is `true` while a run is live, in `get` and `onChange`. Statuses keep their meanings.
- **Backpressure.** An iterable's next item is pulled only once nothing downstream of it (transitively) is queued or running: the `upstreamBusy` rule applied downstream. Manual, held (`stale`), waiting and errored dependents don't hold it; a held live run is pulled freely; a `pause()`d graph pulls nothing.
- **Glitch-free.** A live node between settles isn't busy; a diamond below a live source runs its join once per item, on one consistent version.
- **Ending a live run.** Supersession (a newer run, or an input change), `reset()`, `delete(id)`, a failing input and a spreading skip abort the run's signal **and** call the iterable's `return()`, so a generator's `finally` runs; late items and emits are dropped. A `live(...)` iterable returned by an already-superseded run is closed unread; a plain one is dropped untouched, as before.
- **Resume: `set(id, { resume: true })`** keeps a live run when its inputs change and tells it through **`ctx.changes`** (an async iterable of `{ deps, skipped, changed }` notices, coalesced latest-wins, buffered from the start of the run) and **`ctx.onChange(fn)`** (the callback form). Notices are glitch-free, like runs. A run that isn't live yet, an explicit `run` / `runAll`, and `invalidate` still restart it. New reader `resumes(id)` (`resume()` already un-pauses the graph).
- **Held runs:** a held live run's settles flag dependents `stale` instead of running them.
- Types: `NodeState.live`, `NodeOptions.resume`, `RunContext<R>` with `emit` / `close` / `changes` / `onChange`, `ChangeNotice`, `Live<T>`, `live`, `isLive`, `LIVE`, `RunResult<R>` (admits `Live`, and `AsyncIterable` for an `async function*` run), `Dataflow.resumes`.
- New example `examples/06-live-nodes.mjs`; README section "Live nodes"; Honest limitations updated (producers must cooperate; generator functions are recognized only natively).
- **Upgrading:** under npm's caret rules `^0.0.2` matches only `0.0.2`; change the range to `^0.0.3` (or `0.0.3`).

## 0.0.2 (2026-10-10)

Held runs and a caller-decided equality cut-off (#4, this PR; motivated by johnhenry/miso#43 and johnhenry/miso#48). Additive: a graph that uses neither behaves exactly as in 0.0.1.

- **Held runs: `run(id, { follow: false })` and `runAll({ ids, follow: false })`.** The run's dependents are treated like manual nodes for that run: a done or skipped one keeps its result and is flagged `stale`, none starts. A waiting dependent (already asked to run) and a dependent queued anyway still run; a held run that fails still makes dependents wait; a held run that skips flags its dependents stale instead of spreading. The request stands until the node starts (it survives a wait on a broken input); the latest request wins; `reset()` forgets it. `follow` defaults to `true`. This is what a notebook needs for "run only this cell" and "run up to here" (the caller plans the set from `deps`, `get` and `order`).
- **`UNCHANGED`, `unchanged(result)`, `isUnchanged(value)`.** A `run` that returns `UNCHANGED` (the caller compared its own values) settles `done` with the new **`state.unchanged: true`**; `version` stays, `runId` bumps, `state.result` is kept (or replaced by `unchanged(result)`), and dependents that already ran on the value aren't rerun or flagged stale. Waiting and never-run autorun dependents still run. After anything but a `done` (first run, error, skip, cycle, `reset()`) there is nothing to compare with and it counts as a new value. The scheduler still stores and compares no values.
- **`NodeState.unchanged`** is new (`false` unless the last run returned `UNCHANGED`); `stale` now also marks a dependent a held run didn't rerun. Types: `RunOptions`, `RunAllOptions`, `Unchanged`, `UnchangedWithResult`; `run`'s return type admits `Unchanged<R>`.
- New example `examples/05-partial-runs-and-unchanged.mjs`; README sections "Partial runs" and "Equality cut-off"; Honest limitations updated (equality and partial-run planning stay the caller's).
- **Upgrading:** under npm's caret rules `^0.0.1` matches only `0.0.1`; change the range to `^0.0.2` (or `0.0.2`).

## 0.0.1 (2026-10-10)

Skip values (#2; motivated by johnhenry/miso#5, inspired by Rivet's control-flow-excluded values). Additive: a graph that never skips behaves exactly as in 0.0.0.

- **`SKIP`, `skip(reason)`, `isSkip(value)`.** A `run` that returns `SKIP` (or `skip(reason)`, sync or async) settles its node as the new status **`skipped`**, with `state.skip = { source, reason }`.
- **Skips spread.** Dependents of a skipped node become `skipped` without running (same `source` and `reason`); in a diamond the join is decided once. A spread skip isn't a run: `version` stays, `runId` bumps, and an in-flight run of that node is aborted and dropped.
- **Consumers.** `set(id, { consumesSkip: true })` (or a list of input ids) runs the node anyway; `ctx.skipped` lists the skipped inputs (`[]` otherwise). Coalesce and if/else are documented patterns; there is no first-to-finish race, by glitch-freedom.
- **Unchanged rules, extended:** errors, missing and never-run inputs still make dependents wait, and win over a skip; manual nodes keep their result and turn `stale` (a skipped manual node too); newest run wins for skips; cycles and `reset()` clear skip info; a skipped chain reruns normally once its source produces a value.
- New `consumesSkip(id)` reader; `Status` gains `"skipped"` (a TypeScript `switch` over `Status` that checks exhaustiveness needs a case for it).
- **Upgrading:** under npm's caret rules `^0.0.0` matches only `0.0.0`, so dependents on `^0.0.0` won't get this release; change the range to `^0.0.1` (or `0.0.1`).

## 0.0.0 (2026-10-09)

First release. A new package, never published under another name.

- **`createDataflow({ run, onChange, schedule })`: an async dataflow scheduler that never stores values.** Nodes declare dependencies; an injected async `run(id, { runId, signal, deps })` computes them. States `idle`, `waiting`, `running`, `done`, `error`, plus `version`, `runId`, `stale` and `cycle`.
- **Glitch-free propagation.** A node never starts while anything upstream is queued or running, so a diamond's join runs once per upstream change.
- **Newest run wins.** A rerun aborts the previous run's `AbortSignal` and drops its result even if it settles later.
- **Errors make dependents wait, not fail**, and they rerun by themselves once the error is fixed. A dependent that was queued but had never run used to stay `idle` here and never recover; caught by `examples/03` before release and covered by a regression test.
- **A listener that throws is reported, not fatal.** Like an `EventTarget` listener, an exception from `onChange`/`subscribe` goes to `reportError` and affects neither the other listeners nor scheduling. Before release it surfaced as an unhandled rejection that stalled dependents and left `idle()` pending forever (found while writing the docs); covered by a regression test.
- **States are exactly what listeners see:** `idle`, `waiting`, `running`, `done`, `error`. A node queued behind upstream work keeps its previous state until it starts (an internal `pending` state that was never reported is gone from the docs and types).
- **Manual nodes** (`autorun: false`) keep their result and are flagged `stale` when an input or their definition changes.
- **Cycles** (including self-dependencies) are refused as `error` with `cycle: true` and released when broken.
- Built for the miso spatial notebook (values live in an `@johnhenry/andbox` Worker; previews from `@johnhenry/inspectable`).
