# dataflow examples

Runnable, self-verifying examples. Each one asserts the behavior it demonstrates and exits 0 on success. Values live in plain Maps here; in a real notebook they would live in a Worker or iframe, which changes nothing about the scheduling shown.

| Example | Demonstrates |
| --- | --- |
| [`01-a-spreadsheet-recomputes-only-what-changed.mjs`](./01-a-spreadsheet-recomputes-only-what-changed.mjs) | `invalidate("price")` reruns exactly `price -> tip -> total`, in dependency order, once each; an unrelated cell with no inputs is not rerun. |
| [`02-a-slow-stale-run-never-overwrites-a-newer-one.mjs`](./02-a-slow-stale-run-never-overwrites-a-newer-one.mjs) | A slow first run that settles after a fast second run is dropped: its `AbortSignal` fires and `state.result` keeps the newer value. |
| [`03-errors-make-dependents-wait-not-fail.mjs`](./03-errors-make-dependents-wait-not-fail.mjs) | A failing node leaves its dependents `waiting`, not `error`, and fixing it reruns the whole chain without any extra calls. |
| [`04-skip-values.mjs`](./04-skip-values.mjs) | Two mutually exclusive branches return `SKIP` / `skip(reason)`; the skip spreads to a dependent without running it, a `consumesSkip` coalesce takes whichever branch produced a value, and the skipped chain recovers when its branch applies again. |
| [`05-partial-runs-and-unchanged.mjs`](./05-partial-runs-and-unchanged.mjs) | `run(id, { follow: false })` runs one cell and flags the expensive cell below it `stale` instead of rerunning it; a `run` that returns `UNCHANGED` for an equal value stops propagation. |
| [`06-live-nodes.mjs`](./06-live-nodes.mjs) | A live source (an async generator over a stand-in socket, returned as `live(...)`) settles once per reading, and a burst is shown one reading at a time (backpressure); changing its input restarts it and the old generator's `finally` runs; a `resume: true` history keeps its live run across changes and accumulates every reading through `ctx.changes`. |
| [`07-events-sample-and-queue.mjs`](./07-events-sample-and-queue.mjs) | A send node that samples the draft (`sample`) isn't run by typing; with `queue: true`, a burst of clicks during a slow send is sent once each, in order, none aborted, each run knowing its click from `ctx.trigger`, while a superseding node behind the same button finishes only for the last click; a `queueLimit: 2` queue drops its oldest triggers and counts them in `state.dropped`. |

## Running

```sh
npm run examples      # run all in sequence
npm run example:01    # run one
node examples/01-a-spreadsheet-recomputes-only-what-changed.mjs
```

## Runtime requirements (honest edition)

Plain Node >= 26, no browser needed: the scheduler uses only `queueMicrotask`, `setTimeout`, `AbortController` and async iteration, which Node, browsers and Workers all have.
