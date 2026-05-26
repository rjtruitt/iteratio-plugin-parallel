# iteratio-plugin-parallel

Parallel tool execution plugin for iteratio.

## Install

```
npm install iteratio-plugin-parallel
```

## What It Does

Runs multiple tool calls concurrently instead of sequentially. When the LLM requests several tool calls in a single turn, this plugin executes them in parallel and collects the results. Reduces total execution time for independent operations.

## Usage

```typescript
import { AgentLoop } from 'iteratio';
import { ParallelPlugin } from 'iteratio-plugin-parallel';

const loop = AgentLoop.builder()
  .withLLM(llm)
  .withPlugin(new ParallelPlugin({ maxConcurrency: 5 }))
  .build();
```

## License

MIT
