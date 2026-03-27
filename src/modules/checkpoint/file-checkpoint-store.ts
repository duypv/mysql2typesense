import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname } from "node:path";

import type { BinlogCheckpoint, CheckpointStore } from "../../core/types.js";

export class FileCheckpointStore implements CheckpointStore {
  constructor(private readonly filePath: string) {}

  async load(): Promise<BinlogCheckpoint | null> {
    try {
      const content = await readFile(this.filePath, "utf8");
      return JSON.parse(content) as BinlogCheckpoint;
    } catch (error) {
      const nodeError = error as NodeJS.ErrnoException;
      if (nodeError.code === "ENOENT") {
        return null;
      }

      throw error;
    }
  }

  async save(checkpoint: BinlogCheckpoint): Promise<void> {
    await mkdir(dirname(this.filePath), { recursive: true });
    await writeFile(this.filePath, JSON.stringify(checkpoint, null, 2), "utf8");
  }

  async close(): Promise<void> {}
}