# Project Structure

Muc tieu cua structure nay la tach ro pipeline initial sync va realtime CDC, dong thoi giu cac diem mo rong cho multi-table, checkpoint Redis, transform plugin va dashboard sau nay.

```text
.
├── config/
│   └── sync.config.json
├── docker/
│   └── mysql/
│       └── init/
│           └── 001-schema.sql
├── docker-compose.yml
├── docs/
│   └── project-structure.md
├── storage/
│   └── checkpoints/
├── src/
│   ├── app/
│   │   ├── bootstrap.ts
│   │   ├── bootstrap-sync.ts
│   │   ├── initial-sync.ts
│   │   ├── monitoring-server.ts
│   │   └── realtime-sync.ts
│   ├── config/
│   │   ├── env.ts
│   │   ├── logger.ts
│   │   └── sync-config.ts
│   ├── core/
│   │   └── types.ts
│   ├── modules/
│   │   ├── checkpoint/
│   │   │   ├── checkpoint-store.ts
│   │   │   ├── file-checkpoint-store.ts
│   │   │   └── redis-checkpoint-store.ts
│   │   ├── mysql/
│   │   │   ├── binlog-listener.ts
│   │   │   ├── connection.ts
│   │   │   ├── schema-introspector.ts
│   │   │   └── source-reader.ts
│   │   ├── monitoring/
│   │   │   └── sync-monitor.ts
│   │   ├── sync/
│   │   │   ├── initial-sync.service.ts
│   │   │   └── realtime-sync.service.ts
│   │   │   └── table-config-resolver.ts
│   │   ├── transform/
│   │   │   └── transformer.ts
│   │   └── typesense/
│   │       ├── client.ts
│   │       ├── collection-manager.ts
│   │       └── document-indexer.ts
│   ├── types/
│   │   └── zongji.d.ts
│   └── utils/
│       └── retry.ts
├── .env.example
├── .dockerignore
├── .gitignore
├── Dockerfile
├── package.json
└── tsconfig.json
```

Phan bo trach nhiem:

- `app/`: entrypoint cho tung mode va dependency wiring.
- `config/`: nap env, validate cau hinh, logger va parser config multi-table.
- `core/`: kieu du lieu chung giua MySQL, Typesense va checkpoint.
- `modules/mysql/`: truy van initial sync va lang nghe binlog.
- `modules/mysql/schema-introspector.ts`: tu dong doc schema tu MySQL de fallback sync tat ca bang/cot.
- `modules/monitoring/`: in-memory metrics/state cho healthcheck, metrics va dashboard.
- `modules/typesense/`: tao client, tao collection theo schema config, import/upsert/delete documents.
- `modules/checkpoint/`: file checkpoint va Redis checkpoint cho high availability.
- `modules/transform/`: field mapping va data coercion tu row MySQL sang document Typesense.
- `modules/sync/`: orchestration logic cho initial/realtime va resolver config bang.
- `config/sync.config.json`: khai bao nhieu table cung schema va transform theo tung table.
- `docker-compose.yml`: stack test end-to-end gom MySQL, Redis, Typesense, sync service va dashboard.