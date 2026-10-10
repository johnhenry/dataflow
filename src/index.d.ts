export type Status = "idle" | "waiting" | "running" | "done" | "error";

export interface NodeState<R = unknown> {
  /** Where the node is in its lifecycle. */
  status: Status;
  /** Increments every time a run settles (resolved or rejected). */
  version: number;
  /** Increments every time a run starts, is superseded, or is reset. */
  runId: number;
  /** The last resolved value of `run` (only while `status === "done"`). */
  result?: R;
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
}

export interface DataflowOptions<R = unknown> {
  /** Compute one node. May return a value or a promise. */
  run: (id: string, ctx: RunContext) => R | Promise<R>;
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
}

export interface Dataflow<R = unknown> {
  set(id: string, options?: NodeOptions): this;
  delete(id: string): this;
  has(id: string): boolean;
  get(id: string): NodeState<R> | undefined;
  deps(id: string): string[];
  autorun(id: string): boolean | undefined;
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
