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

export class MySqlBinlogListener implements BinlogListener {
  private readonly zongji: ZongJi;
  private currentCheckpoint: BinlogCheckpoint | null = null;
  private started = false;
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
    this.zongji = new ZongJi({
      host: config.mysql.host,
      port: config.mysql.port,
      user: config.mysql.user,
      password: config.mysql.password,
      database: config.mysql.database
    });
  }

  registerTable(table: TableSyncConfig): void {
    const key = this.tableKey(table.database, table.table);
    if (!this.tableByKey.has(key)) {
      this.tableByKey.set(key, table);
      this.logger?.info({ table: key }, "Registered table for realtime sync");
    }
  }

  async start(onChange: (event: ChangeEvent) => Promise<void>): Promise<void> {
    this.currentCheckpoint = await this.checkpointStore.load();
    // In auto-database mode, always include the configured database name so ZongJi subscribes
    // even before tables are dynamically discovered and registered via registerTable().
    const includeSchema = this.config.sync.database && this.config.sync.tables.length === 0
      ? (() => {
          const dbs = new Set<string>();
          const cfgDb = (this.config.sync.database as { name?: string } | undefined)?.name;
          if (cfgDb) dbs.add(cfgDb);
          for (const table of this.tableByKey.values()) dbs.add(table.database);
          return Array.from(dbs).reduce<Record<string, true | string[]>>((acc, db) => {
            acc[db] = true;
            return acc;
          }, {});
        })()
      : Array.from(this.tableByKey.values()).reduce<Record<string, true | string[]>>((accumulator, table) => {
          const current = accumulator[table.database];
          if (Array.isArray(current)) {
            current.push(table.table);
          } else if (current !== true) {
            accumulator[table.database] = [table.table];
          }
          return accumulator;
        }, {});

    this.logger?.info(
      { checkpoint: this.currentCheckpoint, includeSchema },
      "Binlog listener starting"
    );

    if (Object.keys(includeSchema).length === 0) {
      this.logger?.warn(
        { configDatabase: (this.config.sync.database as { name?: string } | undefined)?.name },
        "No tables registered yet — binlog will capture all schemas and route events once tables are discovered"
      );
    }

    await new Promise<void>((resolve, reject) => {
      const handleError = (error: unknown) => {
        this.logger?.error({ error }, "Binlog listener error");
        reject(error);
      };
      const handleReady = () => {
        this.logger?.info("Binlog listener ready, listening for changes");
        resolve();
      };
      const handleEvent = async (event: RawBinlogEvent) => {
        try {
          this.logger?.debug({ eventName: event.getEventName?.() }, "Binlog event received");
          await this.handleEvent(event, onChange);
        } catch (error) {
          this.logger?.error({ error }, "Error handling binlog event");
          reject(error);
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

      this.started = true;
      this.zongji.start(startOptions);
    });
  }

  async stop(): Promise<void> {
    if (this.started) {
      this.zongji.stop();
      this.started = false;
    }
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