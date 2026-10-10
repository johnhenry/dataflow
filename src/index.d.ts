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
  /** Increments every time a run settles (resolved, skipped by its own run, or rejected). A skip spreading from an input is not a run and doesn't count. */
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
}

export interface RunContext {
  /** Identifies this run; a newer run supersedes it. */
  runId: number;
  /** Aborts when this run is superseded, the node is deleted, or the graph is reset. */
  signal: AbortSignal;
  /** The node's dependency ids at the moment the run started. */
  deps: string[];
  /** The inputs that were skipped when the run started. Always empty unless the node declared `consumesSkip`. */
  skipped: string[];
}

export interface DataflowOptions<R = unknown> {
  /**
   * Compute one node. May return a value or a promise; return `SKIP` or
   * `skip(reason)` to skip the node, `UNCHANGED` or `unchanged(result)` when
   * the value equals the previous one.
   */
  run: (id: string, ctx: RunContext) => R | Skip | Unchanged<R> | Promise<R | Skip | Unchanged<R>>;
  /** Called on every state change of every node. */
  onChange?: (id: string, state: NodeState<R>) => void;
  /** How flushes are deferred. Default `queueMicrotask`. */
  schedule?: (fn: () => void) => void;
}

export interface NodeOptions {
  /** Ids this node reads from. */
  deps?: string[];
  /** Rerun automatically when an input or the definition changes. Default `true`. */
  autorun?: boolean;
  /**
   * Run even when inputs were skipped, instead of being skipped with them.
   * `true` for every input, or the ids of the inputs this node handles (a skip
   * on any other input still spreads). Default `false`. The run sees the
   * skipped inputs in `ctx.skipped`.
   */
  consumesSkip?: boolean | string[];
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
  dependents(id: string): string[];
  /** Topological order (dependencies first); nodes on a cycle are left out. */
  order(ids?: string[]): string[];
  ids(): string[];
  /** Run now, whatever the autorun flag. `follow: false` holds the dependents (flagged stale, not run). */
  run(id: string, options?: RunOptions): this;
  invalidate(id: string): this;
  /** Queue every node (or `ids`) in dependency order. `follow: false` holds dependents outside the set. */
  runAll(options?: RunAllOptions): this;
  reset(): this;
  pause(): this;
  resume(): this;
  readonly paused: boolean;
  subscribe(fn: (id: string, state: NodeState<R>) => void): () => boolean;
  idle(): Promise<void>;
}

export function createDataflow<R = unknown>(options: DataflowOptions<R>): Dataflow<R>;
export default createDataflow;
