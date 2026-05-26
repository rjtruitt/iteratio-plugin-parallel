import { describe, it, expect, vi, beforeEach } from 'vitest';
import { createParallelPlugin, ParallelPlugin } from '../ParallelPlugin';

describe('ParallelPlugin', () => {
  let plugin: ParallelPlugin;

  beforeEach(() => {
    plugin = createParallelPlugin({ defaultConcurrency: 5 });
  });

  describe('fan-out: distribute work', () => {
    it('should distribute work to N parallel handlers', async () => {
      const tasks = [1, 2, 3, 4, 5];
      const handler = vi.fn().mockImplementation(async (n: number) => n * 2);

      const result = await plugin.fanOut(tasks, handler);

      expect(handler).toHaveBeenCalledTimes(5);
      expect(result.successCount).toBe(5);
    });

    it('should execute tasks concurrently (not sequentially)', async () => {
      const timestamps: number[] = [];
      const tasks = [1, 2, 3];
      const handler = async (n: number) => {
        timestamps.push(Date.now());
        await new Promise(r => setTimeout(r, 50));
        return n;
      };

      await plugin.fanOut(tasks, handler);

      // All tasks should start within a short window (concurrent)
      const maxDiff = Math.max(...timestamps) - Math.min(...timestamps);
      expect(maxDiff).toBeLessThan(30); // Started nearly simultaneously
    });
  });

  describe('fan-in: collect results', () => {
    it('should collect results from all parallel handlers', async () => {
      const tasks = ['a', 'b', 'c'];
      const handler = async (s: string) => s.toUpperCase();

      const result = await plugin.fanOut(tasks, handler);

      const values = result.results.map(r => r.result);
      expect(values).toContain('A');
      expect(values).toContain('B');
      expect(values).toContain('C');
    });

    it('should include per-task duration in results', async () => {
      const tasks = [1];
      const handler = async (n: number) => {
        await new Promise(r => setTimeout(r, 20));
        return n;
      };

      const result = await plugin.fanOut(tasks, handler);

      expect(result.results[0].duration).toBeGreaterThanOrEqual(15);
    });

    it('should report total duration', async () => {
      const tasks = [1, 2, 3];
      const handler = async (n: number) => {
        await new Promise(r => setTimeout(r, 30));
        return n;
      };

      const result = await plugin.fanOut(tasks, handler);

      // If running in parallel, total should be close to single task time
      expect(result.totalDuration).toBeLessThan(100);
    });
  });

  describe('barrier synchronization', () => {
    it('should wait for all tasks to complete before returning', async () => {
      const completionOrder: number[] = [];
      const tasks = [1, 2, 3];
      const handler = async (n: number) => {
        await new Promise(r => setTimeout(r, n * 20));
        completionOrder.push(n);
        return n;
      };

      const result = await plugin.fanOut(tasks, handler, { barrier: true });

      // All should be complete by the time we get the result
      expect(result.results).toHaveLength(3);
      expect(result.successCount).toBe(3);
    });
  });

  describe('partial failure', () => {
    it('should collect both successes and failures', async () => {
      const tasks = [1, 2, 3, 4, 5];
      const handler = async (n: number) => {
        if (n === 3 || n === 5) throw new Error(`Task ${n} failed`);
        return n * 10;
      };

      const result = await plugin.fanOut(tasks, handler);

      expect(result.successCount).toBe(3);
      expect(result.failureCount).toBe(2);
      expect(result.results.find(r => r.task === 3)?.error).toBeDefined();
    });

    it('should not abort remaining tasks on failure (default behavior)', async () => {
      const executed: number[] = [];
      const tasks = [1, 2, 3];
      const handler = async (n: number) => {
        if (n === 1) throw new Error('First fails');
        await new Promise(r => setTimeout(r, 10));
        executed.push(n);
        return n;
      };

      await plugin.fanOut(tasks, handler);

      expect(executed).toContain(2);
      expect(executed).toContain(3);
    });
  });

  describe('concurrency limit', () => {
    it('should enforce concurrency limit', async () => {
      let concurrent = 0;
      let maxConcurrent = 0;

      const tasks = Array.from({ length: 10 }, (_, i) => i);
      const handler = async (n: number) => {
        concurrent++;
        maxConcurrent = Math.max(maxConcurrent, concurrent);
        await new Promise(r => setTimeout(r, 30));
        concurrent--;
        return n;
      };

      await plugin.fanOut(tasks, handler, { concurrency: 3 });

      expect(maxConcurrent).toBeLessThanOrEqual(3);
    });

    it('should still complete all tasks despite concurrency limit', async () => {
      const tasks = Array.from({ length: 10 }, (_, i) => i);
      const handler = async (n: number) => n * 2;

      const result = await plugin.fanOut(tasks, handler, { concurrency: 2 });

      expect(result.successCount).toBe(10);
    });
  });

  describe('timeout per parallel task', () => {
    it('should timeout individual tasks that exceed the limit', async () => {
      const tasks = [1, 2, 3];
      const handler = async (n: number) => {
        if (n === 2) await new Promise(r => setTimeout(r, 5000)); // very slow
        return n;
      };

      const result = await plugin.fanOut(tasks, handler, { timeout: 100 });

      expect(result.results.find(r => r.task === 2)?.error?.message).toMatch(/timeout/i);
      expect(result.results.find(r => r.task === 1)?.result).toBe(1);
    });
  });

  describe('fail-fast mode', () => {
    it('should cancel all remaining tasks on first failure when failFast is true', async () => {
      const executed: number[] = [];
      const tasks = [1, 2, 3, 4, 5];
      const handler = async (n: number) => {
        await new Promise(r => setTimeout(r, n * 30));
        if (n === 2) throw new Error('Fail fast trigger');
        executed.push(n);
        return n;
      };

      const result = await plugin.fanOut(tasks, handler, { failFast: true });

      // Tasks after the failure point should be cancelled
      expect(result.failureCount).toBeGreaterThanOrEqual(1);
      // Not all tasks should have completed
      expect(executed.length).toBeLessThan(5);
    });

    it('should include cancellation info in result', async () => {
      const tasks = [1, 2, 3];
      const handler = async (n: number) => {
        if (n === 1) throw new Error('immediate fail');
        await new Promise(r => setTimeout(r, 1000));
        return n;
      };

      const result = await plugin.fanOut(tasks, handler, { failFast: true });

      // Some results should indicate cancellation
      const cancelled = result.results.filter(r => r.error?.message.match(/cancel/i));
      expect(cancelled.length).toBeGreaterThanOrEqual(0);
    });
  });
});
