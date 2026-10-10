# dataflow examples

Runnable, self-verifying examples. Each one asserts the behavior it demonstrates and exits 0 on success. Values live in plain Maps here; in a real notebook they would live in a Worker or iframe, which changes nothing about the scheduling shown.

| Example | Demonstrates |
| --- | --- |
| [`01-a-spreadsheet-recomputes-only-what-changed.mjs`](./01-a-spreadsheet-recomputes-only-what-changed.mjs) | `invalidate("price")` reruns exactly `price -> tip -> total`, in dependency order, once each; an unrelated cell with no inputs is not rerun. |
| [`02-a-slow-stale-run-never-overwrites-a-newer-one.mjs`](./02-a-slow-stale-run-never-overwrites-a-newer-one.mjs) | A slow first run that settles after a fast second run is dropped: its `AbortSignal` fires and `state.result` keeps the newer value. |
| [`03-errors-make-dependents-wait-not-fail.mjs`](./03-errors-make-dependents-wait-not-fail.mjs) | A failing node leaves its dependents `waiting`, not `error`, and fixing it reruns the whole chain without any extra calls. |

## Running

```sh
npm run examples      # run all in sequence
npm run example:01    # run one
node examples/01-a-spreadsheet-recomputes-only-what-changed.mjs
```

## Runtime requirements (honest edition)

Plain Node >= 26, no browser needed: the scheduler uses only `queueMicrotask`, `setTimeout` and `AbortController`, which Node, browsers and Workers all have.
