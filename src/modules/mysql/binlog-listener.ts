import ZongJi from "@powersync/mysql-zongji";

import type {
  AppConfig,
  BinlogCheckpoint,
  BinlogListener,
  ChangeEvent,
  CheckpointStore,
  TableSyncConfig
} from "../../core/types.js";

type RawBinlogEvent = {
  getEventName?: () => string;
  tableMap?: Record<number, { parentSchema?: string; tableName?: string }>;
  tableId?: number;
  rows?: Array<Record<string, unknown> | { before?: Record<string, unknown>; after?: Record<string, unknown> }>;
  nextPosition?: number;
  binlogName?: string;
};

const RECONNECT_BASE_DELAY_MS = 5_000;
const RECONNECT_MAX_DELAY_MS = 60_000;

export class MySqlBinlogListener implements BinlogListener {
  // Assigned in connectZongJi() before any access; '!' suppresses definite-assignment error.
  private zongji!: ZongJi;
  private currentCheckpoint: BinlogCheckpoint | null = null;
  private started = false;
  private connected = false;
  private reconnectAttempt = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private savedOnChange: ((event: ChangeEvent) => Promise<void>) | null = null;
  private readonly tableByKey = new Map<string, TableSyncConfig>();

  private logger: any = null;

  constructor(
    private readonly config: AppConfig,
    private readonly tables: TableSyncConfig[],
    private readonly checkpointStore: CheckpointStore,
    logger?: any
  ) {
    this.logger = logger;
    for (const table of tables) {
      this.tableByKey.set(this.tableKey(table.database, table.table), table);
    }
  }

  /** Whether ZongJi is currently connected and streaming. */
  isConnected(): boolean {
    return this.connected;
  }

  registerTable(table: TableSyncConfig): void {
    const key = this.tableKey(table.database, table.table);
    if (!this.tableByKey.has(key)) {
      this.tableByKey.set(key, table);
      this.logger?.info({ table: key }, "Registered table for realtime sync");
    }
  }

  async start(onChange: (event: ChangeEvent) => Promise<void>): Promise<void> {
    this.savedOnChange = onChange;
    this.started = true;
    this.currentCheckpoint = await this.checkpointStore.load();

    await this.connectZongJi(onChange);
  }

  async stop(): Promise<void> {
    this.started = false;
    this.connected = false;
    if (this.reconnectTimer !== null) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    if (this.zongji) {
      try {
        this.zongji.stop();
      } catch {
        // ignore stop errors
      }
    }
  }

  private createZongJi(): ZongJi {
    return new ZongJi({
      host: this.config.mysql.host,
      port: this.config.mysql.port,
      user: this.config.mysql.user,
      password: this.config.mysql.password,
      database: this.config.mysql.database
    });
  }

  private buildIncludeSchema(): Record<string, true | string[]> {
    if (this.config.sync.database && this.config.sync.tables.length === 0) {
      const dbs = new Set<string>();
      const cfgDb = (this.config.sync.database as { name?: string } | undefined)?.name;
      if (cfgDb) dbs.add(cfgDb);
      for (const table of this.tableByKey.values()) dbs.add(table.database);
      return Array.from(dbs).reduce<Record<string, true | string[]>>((acc, db) => {
        acc[db] = true;
        return acc;
      }, {});
    }
    return Array.from(this.tableByKey.values()).reduce<Record<string, true | string[]>>((accumulator, table) => {
      const current = accumulator[table.database];
      if (Array.isArray(current)) {
        current.push(table.table);
      } else if (current !== true) {
        accumulator[table.database] = [table.table];
      }
      return accumulator;
    }, {});
  }

  /**
   * Creates a fresh ZongJi instance and connects it to the binlog stream.
   * Resolves when ZongJi emits "ready". After ready, a persistent error handler
   * is installed that auto-reconnects on connection loss.
   */
  private async connectZongJi(onChange: (event: ChangeEvent) => Promise<void>): Promise<void> {
    // Always create a fresh ZongJi — a stopped or errored instance cannot be restarted
    this.zongji = this.createZongJi();

    const includeSchema = this.buildIncludeSchema();

    this.logger?.info(
      { checkpoint: this.currentCheckpoint, includeSchema },
      "Binlog listener connecting"
    );

    if (Object.keys(includeSchema).length === 0) {
      this.logger?.warn(
        { configDatabase: (this.config.sync.database as { name?: string } | undefined)?.name },
        "No tables registered yet — binlog will capture all schemas and route events once tables are discovered"
      );
    }

    await new Promise<void>((resolve, reject) => {
      let readyFired = false;

      const handleError = (error: unknown) => {
        const errorMessage = error instanceof Error ? error.message : String(error);
        const isDroppedTableError = errorMessage.includes("Insufficient permissions to access")
          || errorMessage.includes("table has been dropped");

        if (!readyFired) {
          if (isDroppedTableError) {
            this.logger?.warn(
              { error: errorMessage },
              "Binlog references a dropped table during startup — will advance checkpoint to current position and retry"
            );
            this.advanceCheckpointAndReconnect(onChange);
            resolve();
          } else {
            this.logger?.error({ error: errorMessage }, "Binlog listener error during startup");
            reject(error);
          }
        } else if (isDroppedTableError) {
          this.logger?.warn(
            { error: errorMessage },
            "Binlog references a dropped table — advancing checkpoint to current position"
          );
          this.connected = false;
          this.advanceCheckpointAndReconnect(onChange);
        } else {
          this.logger?.error({ error: errorMessage }, "Binlog listener connection lost — scheduling reconnect");
          this.connected = false;
          this.scheduleReconnect(onChange);
        }
      };

      const handleReady = () => {
        readyFired = true;
        this.connected = true;
        this.reconnectAttempt = 0;
        this.logger?.info("Binlog listener ready, listening for changes");
        resolve();
      };

      const handleEvent = async (event: RawBinlogEvent) => {
        try {
          this.logger?.debug({ eventName: event.getEventName?.() }, "Binlog event received");
          await this.handleEvent(event, onChange);
        } catch (error) {
          this.logger?.error({ error }, "Error handling binlog event");
        }
      };

      this.zongji.on("ready", handleReady);
      this.zongji.on("error", handleError);
      this.zongji.on("binlog", handleEvent);

      const startOptions: Record<string, unknown> = {
        includeEvents: ["tablemap", "writerows", "updaterows", "deleterows"],
        includeSchema: includeSchema as Record<string, unknown>
      };

      if (this.currentCheckpoint?.filename && this.currentCheckpoint.position !== undefined) {
        startOptions.filename = this.currentCheckpoint.filename;
        startOptions.position = this.currentCheckpoint.position;
      }

      this.zongji.start(startOptions);
    });
  }

  /**
   * Advances the checkpoint to the current MySQL binlog position and reconnects.
   * Used when the binlog stream references a dropped table that would cause an infinite reconnect loop.
   */
  private advanceCheckpointAndReconnect(onChange: (event: ChangeEvent) => Promise<void>): void {
    try { this.zongji.stop(); } catch { /* ignore */ }

    const delay = RECONNECT_BASE_DELAY_MS;
    this.logger?.info({ delay }, "Will advance checkpoint to current binlog position and reconnect");

    this.reconnectTimer = setTimeout(async () => {
      this.reconnectTimer = null;
      try {
        const mysql2 = await import("mysql2/promise");
        const conn = await mysql2.createConnection({
          host: this.config.mysql.host,
          port: this.config.mysql.port,
          user: this.config.mysql.user,
          password: this.config.mysql.password
        });

        let rows: any[];
        try {
          const [result] = await conn.query("SHOW BINARY LOG STATUS");
          rows = result as any[];
        } catch {
          const [result] = await conn.query("SHOW MASTER STATUS");
          rows = result as any[];
        }
        await conn.end();

        const first = (rows as any[])[0];
        if (first?.File && first?.Position !== undefined) {
          const checkpoint: BinlogCheckpoint = {
            filename: String(first.File),
            position: Number(first.Position),
            updatedAt: new Date().toISOString()
          };
          await this.checkpointStore.save(checkpoint);
          this.currentCheckpoint = checkpoint;
          this.logger?.info(
            { filename: checkpoint.filename, position: checkpoint.position },
            "Advanced checkpoint to current binlog position, skipping dropped-table events"
          );
        }

        await this.connectZongJi(onChange);
        this.reconnectAttempt = 0;
        this.logger?.info("Reconnected successfully after advancing checkpoint");
      } catch (error) {
        const msg = error instanceof Error ? error.message : String(error);
        this.logger?.error({ error: msg }, "Failed to advance checkpoint — falling back to normal reconnect");
        this.scheduleReconnect(onChange);
      }
    }, delay);
  }

  /**
   * Schedules a reconnect attempt with exponential backoff.
   * Creates a brand-new ZongJi instance because ZongJi cannot be restarted after stop/error.
   */
  private scheduleReconnect(onChange: (event: ChangeEvent) => Promise<void>): void {
    if (!this.started || this.reconnectTimer !== null) {
      return;
    }

    const delay = Math.min(
      RECONNECT_BASE_DELAY_MS * Math.pow(2, this.reconnectAttempt),
      RECONNECT_MAX_DELAY_MS
    );
    this.reconnectAttempt += 1;
    this.logger?.info({ delay, attempt: this.reconnectAttempt }, "Scheduling ZongJi reconnect");

    this.reconnectTimer = setTimeout(async () => {
      this.reconnectTimer = null;
      try {
        // Destroy the old instance (it is already dead, but clean up anyway)
        try { this.zongji.stop(); } catch { /* ignore */ }

        // Reload the latest saved checkpoint so we don't replay already-processed events
        this.currentCheckpoint = await this.checkpointStore.load();

        await this.connectZongJi(onChange);
        this.logger?.info("ZongJi reconnected successfully");
      } catch (error) {
        this.logger?.error({ error, attempt: this.reconnectAttempt }, "ZongJi reconnect attempt failed");
        this.scheduleReconnect(onChange);
      }
    }, delay);
  }

  private async handleEvent(
    event: RawBinlogEvent,
    onChange: (event: ChangeEvent) => Promise<void>
  ): Promise<void> {
    const eventName = event.getEventName?.();
    this.logger?.debug({ eventName, rowsCount: event.rows?.length, tableId: event.tableId }, "Processing binlog event");
    if (!eventName || !event.rows?.length) {
      this.logger?.debug({ eventName, hasRows: !!event.rows?.length }, "Skipping event (no name or rows)");
      return;
    }

    const mappedTable = this.resolveTable(event);
    if (!mappedTable) {
      this.logger?.debug({ tableId: event.tableId, eventName }, "Skipping event (table not mapped)");
      return;
    }

    this.logger?.debug({ table: mappedTable.table, eventName, rowCount: event.rows.length }, "Applying event to collection");

    const checkpoint: BinlogCheckpoint = {
      filename: event.binlogName ?? this.currentCheckpoint?.filename,
      position: event.nextPosition ?? this.currentCheckpoint?.position,
      updatedAt: new Date().toISOString()
    };
    this.currentCheckpoint = checkpoint;

    for (const row of event.rows) {
      const normalized =
        typeof row === "object" && row !== null && ("before" in row || "after" in row)
          ? (row as { before?: Record<string, unknown>; after?: Record<string, unknown> })
          : ({
              before: eventName === "deleterows" ? (row as Record<string, unknown>) : undefined,
              after: eventName !== "deleterows" ? (row as Record<string, unknown>) : undefined
            } as { before?: Record<string, unknown>; after?: Record<string, unknown> });

      if (eventName === "deleterows") {
        await onChange({
          operation: "delete",
          table: mappedTable,
          before: normalized.before,
          checkpoint
        });
        continue;
      }

      const mergedAfter = {
        ...(normalized.before ?? {}),
        ...(normalized.after ?? {})
      };

      await onChange({
        operation: "upsert",
        table: mappedTable,
        before: normalized.before,
        after: mergedAfter,
        checkpoint
      });
    }
  }

  private resolveTable(event: RawBinlogEvent): TableSyncConfig | undefined {
    const eventTable = event.tableId !== undefined ? event.tableMap?.[event.tableId] : undefined;
    if (!eventTable?.parentSchema || !eventTable.tableName) {
      return undefined;
    }

    return this.tableByKey.get(this.tableKey(eventTable.parentSchema, eventTable.tableName));
  }

  private tableKey(database: string, table: string): string {
    return `${database}.${table}`;
  }
}