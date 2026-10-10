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
  /** A manual (non-autorun) node whose inputs or definition changed since its last run. */
  stale: boolean;
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
  /** Compute one node. May return a value or a promise; return `SKIP` or `skip(reason)` to skip the node. */
  run: (id: string, ctx: RunContext) => R | Skip | Promise<R | Skip>;
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
  run(id: string): this;
  invalidate(id: string): this;
  runAll(options?: { all?: boolean; ids?: string[] }): this;
  reset(): this;
  pause(): this;
  resume(): this;
  readonly paused: boolean;
  subscribe(fn: (id: string, state: NodeState<R>) => void): () => boolean;
  idle(): Promise<void>;
}

export function createDataflow<R = unknown>(options: DataflowOptions<R>): Dataflow<R>;
export default createDataflow;
