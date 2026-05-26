/** Base plugin contract shared across all iteratio plugins. */
import type { Container } from 'inversify';

export interface IPlugin {
  name: string;
  version: string;
  initialize(container: Container): Promise<void>;
  shutdown(): Promise<void>;
}

/** Tuning knobs for the parallel plugin's default concurrency. */
export interface ParallelConfig {
  defaultConcurrency?: number;
}

/** Per-call options controlling concurrency, timeouts, and failure behavior. */
export interface ParallelOptions {
  concurrency?: number;
  failFast?: boolean;
  /** Per-task timeout in milliseconds. */
  timeout?: number;
  /** When true, all tasks must complete before results are returned. */
  barrier?: boolean;
}

/** Outcome of a single task within a fan-out batch. */
export interface ParallelResultItem<T, R> {
  task: T;
  result?: R;
  error?: Error;
  /** Wall-clock duration in ms for this individual task. */
  duration: number;
}

/** Aggregate outcome of a fan-out operation. */
export interface ParallelResult<T, R> {
  results: Array<ParallelResultItem<T, R>>;
  totalDuration: number;
  successCount: number;
  failureCount: number;
}

/**
 * Provides bounded-concurrency parallel execution ("fan-out") for agent tasks.
 * Supports per-task timeouts, fail-fast cancellation, and configurable concurrency limits.
 */
export class ParallelPlugin implements IPlugin {
  readonly name = 'parallel';
  readonly version = '0.1.0';
  private defaultConcurrency: number;

  /** Create a ParallelPlugin with optional default concurrency configuration. */
  constructor(config?: ParallelConfig) {
    this.defaultConcurrency = config?.defaultConcurrency ?? Infinity;
  }

  /** Initialize the plugin with a dependency injection container. */
  async initialize(_container: Container): Promise<void> {}

  /** Shut down the plugin and release any resources. */
  async shutdown(): Promise<void> {}

  /**
   * Execute tasks in parallel with bounded concurrency.
   * @param tasks - Array of input values to process.
   * @param handler - Async function applied to each task.
   * @param options - Concurrency, timeout, and failure controls.
   */
  async fanOut<T, R>(tasks: T[], handler: (task: T) => Promise<R>, options?: ParallelOptions): Promise<ParallelResult<T, R>> {
    const concurrency = options?.concurrency ?? this.defaultConcurrency;
    const timeout = options?.timeout;
    const failFast = options?.failFast ?? false;

    const startTime = Date.now();
    const results: Array<ParallelResultItem<T, R>> = [];
    let cancelled = false;

    const executeTask = async (task: T): Promise<ParallelResultItem<T, R>> => {
      if (cancelled) {
        return { task, error: new Error('Cancelled'), duration: 0 };
      }
      const taskStart = Date.now();
      try {
        let result: R;
        if (timeout) {
          result = await Promise.race([
            handler(task),
            new Promise<never>((_, reject) => setTimeout(() => reject(new Error('Timeout')), timeout))
          ]);
        } else {
          result = await handler(task);
        }
        if (cancelled) {
          return { task, error: new Error('Cancelled'), duration: Date.now() - taskStart };
        }
        return { task, result, duration: Date.now() - taskStart };
      } catch (e: any) {
        if (failFast) {
          cancelled = true;
        }
        return { task, error: e, duration: Date.now() - taskStart };
      }
    };

    if (concurrency >= tasks.length) {
      const settled = await Promise.all(tasks.map(t => executeTask(t)));
      results.push(...settled);
    } else {
      let index = 0;
      const executing: Promise<void>[] = [];

      const enqueue = (): Promise<void> => {
        if (index >= tasks.length || cancelled) return Promise.resolve();
        const task = tasks[index++];
        const p = executeTask(task).then(r => {
          results.push(r);
          return enqueue();
        });
        return p;
      };

      for (let i = 0; i < concurrency; i++) {
        executing.push(enqueue());
      }
      await Promise.all(executing);
    }

    const totalDuration = Date.now() - startTime;
    const successCount = results.filter(r => !r.error).length;
    const failureCount = results.filter(r => !!r.error).length;

    return { results, totalDuration, successCount, failureCount };
  }
}

/** Convenience factory for the parallel plugin. */
export function createParallelPlugin(config?: ParallelConfig): ParallelPlugin {
  return new ParallelPlugin(config);
}
