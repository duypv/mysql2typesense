import type { RetryConfig } from "../core/types.js";

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function withRetry<T>(operation: () => Promise<T>, config: RetryConfig): Promise<T> {
  let attempt = 0;
  let lastError: unknown;

  while (attempt < config.maxAttempts) {
    attempt += 1;
    try {
      return await operation();
    } catch (error) {
      lastError = error;
      if (attempt >= config.maxAttempts) {
        break;
      }

      await sleep(config.baseDelayMs * attempt);
    }
  }

  throw lastError;
}