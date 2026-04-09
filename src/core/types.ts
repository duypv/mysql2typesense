export type TypesenseFieldType =
  | "string"
  | "int32"
  | "int64"
  | "float"
  | "bool"
  | "geopoint"
  | "geopolygon"
  | "geopoint[]"
  | "string[]"
  | "int32[]"
  | "int64[]"
  | "float[]"
  | "bool[]"
  | "object"
  | "object[]"
  | "auto"
  | "string*"
  | "image";

export interface TypesenseFieldConfig {
  name: string;
  type: TypesenseFieldType;
  optional?: boolean;
  facet?: boolean;
  index?: boolean;
  sort?: boolean;
  locale?: string;
  infix?: boolean;
  stem?: boolean;
  num_dim?: number;
  store?: boolean;
  range_index?: boolean;
  /** Typesense join reference — format: "CollectionName.fieldName" */
  reference?: string;
  /** If true the document is accepted even if the referenced document does not exist yet */
  async_reference?: boolean;
  [key: string]: unknown;
}

export interface TypesenseCollectionConfig {
  fields: TypesenseFieldConfig[];
  defaultSortingField?: string;
  enableNestedFields?: boolean;
  tokenSeparators?: string[];
  symbolsToIndex?: string[];
}

export type TransformSourceFormat = "plain" | "json" | "csv" | "datetime";

export interface TransformFieldMapping {
  source: string;
  target: string;
  type: TypesenseFieldType;
  optional?: boolean;
  defaultValue?: unknown;
  sourceFormat?: TransformSourceFormat;
  arraySeparator?: string;
  timestampResolution?: "seconds" | "milliseconds";
}

export interface TableTransformConfig {
  fieldMappings: TransformFieldMapping[];
  dropNulls?: boolean;
}

export interface TableSyncConfigSeed {
  database?: string;
  table: string;
  primaryKey?: string;
  collection?: string;
  batchSize?: number;
  typesense?: Partial<TypesenseCollectionConfig>;
  transform?: Partial<TableTransformConfig>;
}

export interface TableSyncConfig {
  database: string;
  table: string;
  primaryKey: string;
  collection: string;
  batchSize?: number;
  typesense: TypesenseCollectionConfig;
  transform: TableTransformConfig;
}

export interface DatabaseSyncConfig {
  name: string;
  excludeFields?: string[];
  infixString?: boolean;
  jsonStringify?: string[];
  facetFields?: string[];
}

export interface JoinFieldConfig {
  /** Column name in the MySQL table */
  name: string;
  /** Typesense join reference — format: "CollectionName.fieldName". Optional when only a type override is needed. */
  reference?: string;
  /** Accept document even if referenced document does not exist yet */
  async_reference?: boolean;
  /** Override the inferred Typesense field type */
  type?: TypesenseFieldType;
}

export interface TableJoinConfig {
  /** MySQL table name (case-insensitive match) */
  table: string;
  fields: JoinFieldConfig[];
}

export interface SyncConfigFile {
  database?: DatabaseSyncConfig;
  tables?: TableSyncConfigSeed[];
  join_configs?: TableJoinConfig[];
}

export interface SyncDocument extends Record<string, unknown> {
  id: string;
}

export interface BinlogCheckpoint {
  filename?: string;
  position?: number;
  updatedAt: string;
}

export type ChangeOperation = "upsert" | "delete";

export interface ChangeEvent {
  operation: ChangeOperation;
  table: TableSyncConfig;
  before?: Record<string, unknown>;
  after?: Record<string, unknown>;
  checkpoint?: BinlogCheckpoint;
}

export interface RetryConfig {
  maxAttempts: number;
  baseDelayMs: number;
}

export interface AppConfig {
  mysql: {
    host: string;
    port: number;
    user: string;
    password: string;
    database: string;
  };
  sync: {
    batchSize: number;
    database?: DatabaseSyncConfig;
    tables: TableSyncConfigSeed[];
    retry: RetryConfig;
    joinConfigs: TableJoinConfig[];
  };
  typesense: {
    host: string;
    port: number;
    protocol: "http" | "https";
    apiKey: string;
  };
  checkpoint: {
    driver: "file" | "redis";
    filePath?: string;
    redisUrl?: string;
    redisKey?: string;
  };
  monitoring: {
    host: string;
    port: number;
    enabled: boolean;
    authToken?: string;
  };
  logLevel: string;
}

export interface ThroughputPoint {
  at: string;
  upserts: number;
  deletes: number;
}

export interface SyncMonitorSnapshot {
  startedAt: string;
  mode: "idle" | "initial" | "realtime";
  tables: string[];
  counters: {
    initialBatches: number;
    initialDocuments: number;
    realtimeUpserts: number;
    realtimeDeletes: number;
    errors: number;
  };
  perTable: Record<string, { initialDocuments: number; upserts: number; deletes: number }>;
  recentErrors: Array<{ at: string; message: string; context?: string; data?: Record<string, unknown> }>;
  throughput: ThroughputPoint[];
}

export interface SyncMonitor {
  setTables(tables: TableSyncConfig[]): void;
  markMode(mode: "idle" | "initial" | "realtime"): void;
  recordInitialBatch(table: string, count: number): void;
  recordRealtimeEvent(table: string, operation: ChangeOperation): void;
  recordError(error: unknown, context?: string, data?: Record<string, unknown>): void;
  snapshot(): SyncMonitorSnapshot;
  toPrometheusMetrics(): string;
}

export interface CheckpointStore {
  load(): Promise<BinlogCheckpoint | null>;
  save(checkpoint: BinlogCheckpoint): Promise<void>;
  close(): Promise<void>;
}

export interface DocumentTransformer {
  toDocument(row: Record<string, unknown>, table: TableSyncConfig): Promise<SyncDocument>;
}

export interface BinlogListener {
  start(onChange: (event: ChangeEvent) => Promise<void>): Promise<void>;
  stop(): Promise<void>;
  registerTable?(table: TableSyncConfig): void;
}