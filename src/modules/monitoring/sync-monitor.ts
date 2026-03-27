import type { ChangeOperation, SyncMonitor, SyncMonitorSnapshot, TableSyncConfig, ThroughputPoint } from "../../core/types.js";

export class InMemorySyncMonitor implements SyncMonitor {
  private static readonly MAX_THROUGHPUT_POINTS = 120;
  private readonly startedAt = new Date().toISOString();
  private mode: "idle" | "initial" | "realtime" = "idle";
  private tables: string[] = [];
  private readonly counters = {
    initialBatches: 0,
    initialDocuments: 0,
    realtimeUpserts: 0,
    realtimeDeletes: 0,
    errors: 0
  };
  private readonly perTable: Record<string, { initialDocuments: number; upserts: number; deletes: number }> = {};
  private readonly recentErrors: Array<{ at: string; message: string; context?: string }> = [];
  private readonly throughput: ThroughputPoint[] = [];

  private ensureCurrentThroughputPoint(): ThroughputPoint {
    const currentSecond = new Date().toISOString().slice(0, 19) + "Z";
    const last = this.throughput[this.throughput.length - 1];
    if (last?.at === currentSecond) {
      return last;
    }

    const point: ThroughputPoint = { at: currentSecond, upserts: 0, deletes: 0 };
    this.throughput.push(point);
    if (this.throughput.length > InMemorySyncMonitor.MAX_THROUGHPUT_POINTS) {
      this.throughput.shift();
    }
    return point;
  }

  setTables(tables: TableSyncConfig[]): void {
    this.tables = tables.map((table) => `${table.database}.${table.table}`);
    for (const table of this.tables) {
      this.perTable[table] ??= { initialDocuments: 0, upserts: 0, deletes: 0 };
    }
  }

  markMode(mode: "idle" | "initial" | "realtime"): void {
    this.mode = mode;
  }

  recordInitialBatch(table: string, count: number): void {
    this.counters.initialBatches += 1;
    this.counters.initialDocuments += count;
    this.perTable[table] ??= { initialDocuments: 0, upserts: 0, deletes: 0 };
    this.perTable[table].initialDocuments += count;
  }

  recordRealtimeEvent(table: string, operation: ChangeOperation): void {
    this.perTable[table] ??= { initialDocuments: 0, upserts: 0, deletes: 0 };
    const throughputPoint = this.ensureCurrentThroughputPoint();
    if (operation === "upsert") {
      this.counters.realtimeUpserts += 1;
      this.perTable[table].upserts += 1;
      throughputPoint.upserts += 1;
      return;
    }

    this.counters.realtimeDeletes += 1;
    this.perTable[table].deletes += 1;
    throughputPoint.deletes += 1;
  }

  recordError(error: unknown, context?: string): void {
    this.counters.errors += 1;
    this.recentErrors.unshift({
      at: new Date().toISOString(),
      message: error instanceof Error ? error.message : String(error),
      context
    });
    if (this.recentErrors.length > 20) {
      this.recentErrors.length = 20;
    }
  }

  snapshot(): SyncMonitorSnapshot {
    return {
      startedAt: this.startedAt,
      mode: this.mode,
      tables: this.tables,
      counters: { ...this.counters },
      perTable: { ...this.perTable },
      recentErrors: [...this.recentErrors],
      throughput: [...this.throughput]
    };
  }

  toPrometheusMetrics(): string {
    const lines = [
      "# HELP mysql2typesense_initial_batches_total Number of initial sync batches.",
      "# TYPE mysql2typesense_initial_batches_total counter",
      `mysql2typesense_initial_batches_total ${this.counters.initialBatches}`,
      "# HELP mysql2typesense_initial_documents_total Number of documents imported during initial sync.",
      "# TYPE mysql2typesense_initial_documents_total counter",
      `mysql2typesense_initial_documents_total ${this.counters.initialDocuments}`,
      "# HELP mysql2typesense_realtime_upserts_total Number of realtime upsert events.",
      "# TYPE mysql2typesense_realtime_upserts_total counter",
      `mysql2typesense_realtime_upserts_total ${this.counters.realtimeUpserts}`,
      "# HELP mysql2typesense_realtime_deletes_total Number of realtime delete events.",
      "# TYPE mysql2typesense_realtime_deletes_total counter",
      `mysql2typesense_realtime_deletes_total ${this.counters.realtimeDeletes}`,
      "# HELP mysql2typesense_errors_total Number of sync errors.",
      "# TYPE mysql2typesense_errors_total counter",
      `mysql2typesense_errors_total ${this.counters.errors}`
    ];

    return `${lines.join("\n")}\n`;
  }
}
