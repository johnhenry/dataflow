export type Status = "idle" | "waiting" | "running" | "done" | "skipped" | "error";

/** Return this from `run` to decline to produce a value (the node settles as `skipped`). `Symbol.for("@johnhenry/dataflow.skip")`. */
export const SKIP: unique symbol;

/** What `skip(reason)` returns: a skip that carries a reason. */
export interface SkipWithReason {
  readonly [SKIP]: true;
  readonly reason?: unknown;
}

/** Anything `run` can return to skip: `SKIP` or `skip(reason)`. */
export type Skip = typeof SKIP | SkipWithReason;

/** A skip with a reason; dependents see it in `state.skip.reason`. */
export function skip(reason?: unknown): SkipWithReason;

/** True for `SKIP` and for what `skip(reason)` returns. */
export function isSkip(value: unknown): value is Skip;

/** Return this from `run` when the new value equals the previous one (the caller compares). `Symbol.for("@johnhenry/dataflow.unchanged")`. */
export const UNCHANGED: unique symbol;

/** What `unchanged(result)` returns: an unchanged value that also replaces `state.result`. */
export interface UnchangedWithResult<R = unknown> {
  readonly [UNCHANGED]: true;
  readonly result: R;
}

/** Anything `run` can return to say "same value as before": `UNCHANGED` or `unchanged(result)`. */
export type Unchanged<R = unknown> = typeof UNCHANGED | UnchangedWithResult<R>;

/** An unchanged value whose `result` replaces `state.result` (a bare `UNCHANGED` keeps the previous one). */
export function unchanged<R>(result: R): UnchangedWithResult<R>;

/** True for `UNCHANGED` and for what `unchanged(result)` returns. */
export function isUnchanged(value: unknown): value is Unchanged;

/** Why a node is skipped (present only while `status === "skipped"`). */
export interface SkipInfo {
  /** The node whose run returned the skip (the node itself, or the upstream node it spread from). */
  readonly source: string;
  /** The reason given to `skip(reason)`; `undefined` for a bare `SKIP`. */
  readonly reason?: unknown;
}

export interface NodeState<R = unknown> {
  /** Where the node is in its lifecycle. */
  status: Status;
  /** Increments every time a run settles (resolved, skipped by its own run, or rejected), and on every settle (item or emit) of a live run except `UNCHANGED` ones. A skip spreading from an input is not a run and doesn't count. */
  version: number;
  /** Increments every time a run starts, is superseded (including by a skip spreading from an input), or is reset. */
  runId: number;
  /** The last resolved value of `run` (only while `status === "done"`). */
  result?: R;
  /** Present only while `status === "skipped"`: where the skip came from, and why. */
  skip?: SkipInfo;
  /** The last rejection reason, or the cycle error. */
  error?: unknown;
  /**
   * A manual (non-autorun) node whose inputs or definition changed since its
   * last run, or a dependent held by a `follow: false` run.
   */
  stale: boolean;
  /**
   * The last run returned `UNCHANGED` (after a `done` run): `version` didn't
   * bump and dependents weren't rerun. Cleared when the next run starts.
   */
  unchanged: boolean;
  /** True while the node sits on a dependency cycle. */
  cycle: boolean;
  /**
   * True while the node's run is live: it has emitted (`ctx.emit`) or returned
   * `live(iterable)`, and hasn't ended (iterable finished, promise resolved,
   * `ctx.close()`, an error, or superseded/reset/deleted). `status` keeps its
   * meaning: `running` until the first settle, then the last settle's status.
   */
  live: boolean;
  /** Present only on a node set with `queue: true`: how many triggers wait behind the run in flight. */
  queued?: number;
  /** Present only on a node set with `queue: true`: triggers dropped because the queue was at `queueLimit` (oldest first). Reset by `reset()`. */
  dropped?: number;
}

/**
 * Why a queued run (a node set with `queue: true`) was started: `ctx.trigger`.
 * Frozen. `settle`: a triggering dependency settled; `run`: an explicit
 * `run(id)` / `runAll(...)`; `invalidate`: `invalidate(id)`.
 */
export type Trigger<R = unknown> =
  | {
      readonly type: "settle";
      /** The dependency that settled. */
      readonly id: string;
      /** Its status then: `"done"`, or `"skipped"` (only seen by a node that consumes the skip). */
      readonly status: Status;
      /** Its `version` then. */
      readonly version: number;
      /** Its `state.result` then: the same reference, not a copy (the scheduler stores no values). */
      readonly result: R | undefined;
      /** Its `state.skip` then, for a skipped settle. */
      readonly skip?: SkipInfo;
    }
  | { readonly type: "run" }
  | { readonly type: "invalidate" };

/** What a live `resume` run is told when its inputs changed (instead of being restarted). Frozen. */
export interface ChangeNotice {
  /** The node's dependency ids now (they may differ from `ctx.deps` after a `set`). */
  readonly deps: string[];
  /** The inputs that are skipped now (only ever non-empty for a node with `consumesSkip`). */
  readonly skipped: string[];
  /** The inputs that settled since the last notice (coalesced notices merge these). */
  readonly changed: string[];
}

export interface RunContext<R = unknown> {
  /** Identifies this run; a newer run supersedes it. A live run keeps its `runId` across settles. */
  runId: number;
  /** Aborts when this run is superseded, the node is deleted, or the graph is reset (live runs too: their iterable's `return()` is called as well). */
  signal: AbortSignal;
  /** The node's input ids at the moment the run started: its `deps`, then its sampled inputs. */
  deps: string[];
  /** The inputs that were skipped when the run started. Always empty unless the node declared `consumesSkip`. */
  skipped: string[];
  /** The sampled inputs (read, not triggering), also listed in `deps`. Empty for a node without `sample`. */
  sampled: string[];
  /**
   * Only for a node set with `queue: true`: the trigger this run is for (which
   * dependency settled, its version and result reference; or an explicit run).
   * Absent otherwise.
   */
  trigger?: Trigger<R>;
  /**
   * Settle the node now, without ending the run (the push form of a live run;
   * no backpressure). A value settles `done`, `SKIP`/`skip(reason)` settles
   * `skipped`, `UNCHANGED`/`unchanged(result)` works as when returned. Returns
   * `false`, and does nothing, once the run is superseded, reset, deleted,
   * closed or ended. After the first emit the run's own resolved value no
   * longer settles: resolving just ends the run (`live: false`).
   */
  emit(result: R | Skip | Unchanged<R>): boolean;
  /** End this live run now: later emits are refused, an open iterable is closed (`return()`). Not a cancellation: the signal doesn't abort. */
  close(): void;
  /**
   * Dependency-change notices for a live run of a node set with `resume: true`
   * (instead of a restart). A mailbox of one, coalesced latest-wins, kept from
   * the start of the run; meant for one consumer. Never yields for other
   * nodes. Ends when the run ends.
   */
  changes: AsyncIterable<ChangeNotice>;
  /** The callback form of `changes`: called with every notice (not coalesced). Returns an unsubscribe function. */
  onChange(fn: (notice: ChangeNotice) => void): () => boolean;
}

/** Return this from `run` to make the run live. `Symbol.for("@johnhenry/dataflow.live")`. */
export const LIVE: unique symbol;

/** What `live(iterable)` returns: an async iterable marked as a live run. */
export interface Live<T = unknown> {
  readonly [LIVE]: true;
  readonly iterable: AsyncIterable<T>;
}

/**
 * Mark an async iterable as a live run: returned from `run`, each item settles
 * the node. Live runs are opt-in: an async iterable returned without it (a
 * `ReadableStream`, a generator object) is a plain value. Throws a `TypeError`
 * for anything that isn't an async iterable.
 */
export function live<T>(iterable: AsyncIterable<T>): Live<T>;

/** True for what `live(iterable)` returns. */
export function isLive(value: unknown): value is Live;

/**
 * What `run` may return (or resolve to). `live(iterable)` makes the run live.
 * The `AsyncIterable` member is for a `run` that is itself an `async function*`
 * (live without the marker); from any other `run` an async iterable is a plain
 * value -- the type can't tell the two apart.
 */
export type RunResult<R = unknown> = R | Skip | Unchanged<R> | Live<R | Skip | Unchanged<R>> | AsyncIterable<R | Skip | Unchanged<R>>;

export interface DataflowOptions<R = unknown> {
  /**
   * Compute one node. May return a value or a promise; return `SKIP` or
   * `skip(reason)` to skip the node, `UNCHANGED` or `unchanged(result)` when
   * the value equals the previous one. Return `live(iterable)`, or make `run`
   * itself an `async function*`, to make the run live (opt-in): each item
   * settles the node, the next item is pulled once everything downstream of
   * the previous one settled. Any other async iterable is a plain value.
   */
  run: (id: string, ctx: RunContext<R>) => RunResult<R> | Promise<RunResult<R>>;
  /** Called on every state change of every node. */
  onChange?: (id: string, state: NodeState<R>) => void;
  /** How flushes are deferred. Default `queueMicrotask`. */
  schedule?: (fn: () => void) => void;
}

export interface NodeOptions {
  /** Ids this node reads from; a settle of any of them triggers it. */
  deps?: string[];
  /**
   * Ids this node reads from without being triggered by them (`withLatestFrom`).
   * They order and gate the node like `deps` (it never starts while one is
   * queued or running, and waits until each has settled; a failing or waiting
   * one makes it wait, a skipped one spreads its skip unless consumed), but
   * their settles never run, stale, invalidate or notify it. `ctx.deps` lists
   * them after `deps`; `ctx.sampled` names them. An id in both lists triggers.
   * Default `[]`.
   */
  sample?: string[];
  /** Rerun automatically when an input or the definition changes. Default `true`. */
  autorun?: boolean;
  /**
   * Run even when inputs were skipped, instead of being skipped with them.
   * `true` for every input, or the ids of the inputs this node handles (a skip
   * on any other input still spreads). Default `false`. The run sees the
   * skipped inputs in `ctx.skipped`.
   */
  consumesSkip?: boolean | string[];
  /**
   * While the node's run is live, an input change doesn't restart it: the run
   * is told through `ctx.changes` / `ctx.onChange` and decides what to do. A
   * run that isn't live restarts as usual, and so does an explicit `run`,
   * `runAll` or `invalidate` of the node. Default `false`.
   */
  resume?: boolean;
  /**
   * Queue each trigger (a settle of a dependency in `deps`, or an explicit
   * `run` / `runAll` / `invalidate`) instead of superseding: runs go one after
   * another, in order, none aborted by a newer trigger, each with
   * `ctx.trigger`. `reset()`, `delete(id)` and `run(id, { replace: true })`
   * clear the queue. Can't be combined with `resume`. Default `false`.
   */
  queue?: boolean;
  /** With `queue`: how many triggers may wait; past it the oldest is dropped (`state.dropped` +1). A positive integer or `Infinity`. Default `1000`. */
  queueLimit?: number;
}

export interface RunOptions {
  /**
   * Whether autorun dependents follow this run. Default `true`. With `false`
   * every dependent that isn't queued itself is treated like a manual node for
   * this run: a done or skipped one keeps its result and is flagged `stale`;
   * a waiting one (already asked to run) still runs. The request stands until
   * the node starts, so a node waiting on a broken input keeps it.
   */
  follow?: boolean;
}

export interface RunNodeOptions extends RunOptions {
  /**
   * For a node set with `queue: true`: drop its queued triggers and supersede
   * the run in flight, as a node without `queue` always does. No effect on
   * other nodes. Default `false`.
   */
  replace?: boolean;
}

export interface RunAllOptions extends RunOptions {
  /** `false` skips manual nodes. Default `true`. */
  all?: boolean;
  /** Only these nodes (still in dependency order). Default: every node. */
  ids?: string[];
}

export interface Dataflow<R = unknown> {
  set(id: string, options?: NodeOptions): this;
  delete(id: string): this;
  has(id: string): boolean;
  get(id: string): NodeState<R> | undefined;
  deps(id: string): string[];
  autorun(id: string): boolean | undefined;
  consumesSkip(id: string): boolean | string[] | undefined;
  /** The node's `resume` option (named `resumes` because `resume()` un-pauses the graph). */
  resumes(id: string): boolean | undefined;
  /** The node's sampled inputs (`sample`, without ids that are also in `deps`). */
  samples(id: string): string[] | undefined;
  /** The node's `queue` option. */
  queues(id: string): boolean | undefined;
  /** The node's `queueLimit` option (the default for a node without `queue`). */
  queueLimit(id: string): number | undefined;
  /** The nodes `id` triggers (through their `deps`); nodes that only sample it aren't listed. */
  dependents(id: string): string[];
  /** Topological order (dependencies first); nodes on a cycle are left out. */
  order(ids?: string[]): string[];
  ids(): string[];
  /** Run now, whatever the autorun flag. `follow: false` holds the dependents (flagged stale, not run). A `queue` node queues the run; `replace: true` supersedes instead. */
  run(id: string, options?: RunNodeOptions): this;
  invalidate(id: string): this;
  /** Queue every node (or `ids`) in dependency order. `follow: false` holds dependents outside the set. */
  runAll(options?: RunAllOptions): this;
  reset(): this;
  /** Stop starting runs. Running ones still settle (and emit); live iterables aren't pulled. */
  pause(): this;
  resume(): this;
  readonly paused: boolean;
  subscribe(fn: (id: string, state: NodeState<R>) => void): () => boolean;
  idle(): Promise<void>;
}

export function createDataflow<R = unknown>(options: DataflowOptions<R>): Dataflow<R>;
export default createDataflow;
