import type { BinlogCheckpoint, CheckpointStore } from "../../core/types.js";

interface RedisLikeClient {
  isOpen: boolean;
  get(key: string): Promise<string | null>;
  set(key: string, value: string): Promise<unknown>;
  quit(): Promise<unknown>;
}

export class RedisCheckpointStore implements CheckpointStore {
  constructor(
    private readonly client: RedisLikeClient,
    private readonly key: string
  ) {}

  async load(): Promise<BinlogCheckpoint | null> {
    const raw = await this.client.get(this.key);
    return raw ? (JSON.parse(raw) as BinlogCheckpoint) : null;
  }

  async save(checkpoint: BinlogCheckpoint): Promise<void> {
    await this.client.set(this.key, JSON.stringify(checkpoint));
  }

  async close(): Promise<void> {
    if (this.client.isOpen) {
      await this.client.quit();
    }
  }
}
