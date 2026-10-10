# Changelog

## 0.0.0 (2026-10-09)

First release. A new package, never published under another name.

- **`createDataflow({ run, onChange, schedule })`: an async dataflow scheduler that never stores values.** Nodes declare dependencies; an injected async `run(id, { runId, signal, deps })` computes them. States `idle`, `waiting`, `running`, `done`, `error`, plus `version`, `runId`, `stale` and `cycle`.
- **Glitch-free propagation.** A node never starts while anything upstream is queued or running, so a diamond's join runs once per upstream change.
- **Newest run wins.** A rerun aborts the previous run's `AbortSignal` and drops its result even if it settles later.
- **Errors make dependents wait, not fail**, and they rerun by themselves once the error is fixed. A dependent that was queued but had never run used to stay `idle` here and never recover; caught by `examples/03` before release and covered by a regression test.
- **Manual nodes** (`autorun: false`) keep their result and are flagged `stale` when an input or their definition changes.
- **Cycles** (including self-dependencies) are refused as `error` with `cycle: true` and released when broken.
- Built for the miso spatial notebook (values live in an `@johnhenry/andbox` Worker; previews from `@johnhenry/inspectable`).
