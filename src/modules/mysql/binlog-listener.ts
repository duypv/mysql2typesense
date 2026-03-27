import ZongJi from "zongji";

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
  rows?: Array<{ before?: Record<string, unknown>; after?: Record<string, unknown> }>;
  nextPosition?: number;
  binlogName?: string;
};

export class MySqlBinlogListener implements BinlogListener {
  private readonly zongji: ZongJi;
  private currentCheckpoint: BinlogCheckpoint | null = null;
  private started = false;

  constructor(
    private readonly config: AppConfig,
    private readonly tables: TableSyncConfig[],
    private readonly checkpointStore: CheckpointStore
  ) {
    this.zongji = new ZongJi({
      host: config.mysql.host,
      port: config.mysql.port,
      user: config.mysql.user,
      password: config.mysql.password,
      database: config.mysql.database
    });
  }

  async start(onChange: (event: ChangeEvent) => Promise<void>): Promise<void> {
    this.currentCheckpoint = await this.checkpointStore.load();
    const includeSchema = this.tables.reduce<Record<string, string[]>>((accumulator, table) => {
      accumulator[table.database] ??= [];
      accumulator[table.database].push(table.table);
      return accumulator;
    }, {});

    await new Promise<void>((resolve, reject) => {
      const handleError = (error: unknown) => reject(error);
      const handleReady = () => resolve();
      const handleEvent = async (event: RawBinlogEvent) => {
        try {
          await this.handleEvent(event, onChange);
        } catch (error) {
          reject(error);
        }
      };

      this.zongji.on("ready", handleReady);
      this.zongji.on("error", handleError);
      this.zongji.on("binlog", handleEvent);

      this.started = true;
      this.zongji.start({
        includeEvents: ["writerows", "updaterows", "deleterows"],
        includeSchema,
        filename: this.currentCheckpoint?.filename,
        position: this.currentCheckpoint?.position
      });
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
    if (!eventName || !event.rows?.length) {
      return;
    }

    const mappedTable = this.resolveTable(event);
    if (!mappedTable) {
      return;
    }

    const checkpoint: BinlogCheckpoint = {
      filename: event.binlogName ?? this.currentCheckpoint?.filename,
      position: event.nextPosition ?? this.currentCheckpoint?.position,
      updatedAt: new Date().toISOString()
    };
    this.currentCheckpoint = checkpoint;

    for (const row of event.rows) {
      if (eventName === "deleterows") {
        await onChange({
          operation: "delete",
          table: mappedTable,
          before: row.before,
          checkpoint
        });
        continue;
      }

      await onChange({
        operation: "upsert",
        table: mappedTable,
        before: row.before,
        after: row.after,
        checkpoint
      });
    }
  }

  private resolveTable(event: RawBinlogEvent): TableSyncConfig | undefined {
    const eventTable = event.tableId !== undefined ? event.tableMap?.[event.tableId] : undefined;

    return this.tables.find(
      (table) => table.database === eventTable?.parentSchema && table.table === eventTable?.tableName
    );
  }
}