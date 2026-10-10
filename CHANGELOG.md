# Changelog

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
