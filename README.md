# MySQL to Typesense CDC Sync

Lightweight TypeScript service to keep Typesense in sync with MySQL using binlog-based CDC.

## What This Service Does

The service runs in two phases:

1. Initial sync:
- Reads existing rows from MySQL in batches.
- Creates/updates documents in Typesense.

2. Realtime sync:
- Listens to MySQL binlog events (insert/update/delete).
- Applies changes to Typesense continuously.

## Current Features (Code-Accurate)

- Multi-table sync from `config/sync.config.json`.
- Database auto mode:
- If `database` is configured and `tables` is empty, the service auto-discovers and syncs all tables in that database.
- Runtime table discovery:
- New tables added later are discovered and synced automatically.
- Field exclusion in auto mode:
- `database.excludeFields` removes sensitive fields from schema/mapping.
- String infix default in auto mode:
- `database.infix_string: true` applies `infix: true` to all string/string* fields by default.
- Flexible transform mapping (`plain`, `csv`, `json`, `datetime`).
- Checkpoint store:
- `file` or `redis` for binlog resume position.
- Monitoring server:
- `GET /health`
- `GET /metrics` (Prometheus format)
- `GET /api/status`
- `GET /api/collections`
- `DELETE /api/collections/:name`
- `POST /api/reindex/:name`
- `GET /api/discovered-tables`
- Built-in dashboard at `GET /dashboard`.
- Optional Basic Auth for dashboard/admin endpoints via `MONITORING_AUTH_TOKEN`.

## Requirements

- Node.js 18+
- MySQL 5.7+ or 8.0+ with ROW binlog
- Typesense server with API key
- Redis (optional but recommended for checkpoints)

## MySQL Requirements

Enable binlog with ROW format in MySQL config:

```ini
[mysqld]
server-id        = 1
log_bin          = /var/log/mysql/mysql-bin.log
binlog_format    = ROW
binlog_row_image = FULL
```

Required MySQL privileges for the sync user:

- `REPLICATION SLAVE` (or equivalent in your MySQL version)
- `REPLICATION CLIENT`
- Read access on source tables

## Installation (Local)

```bash
git clone https://github.com/duypv/mysql2typesense.git
cd mysql2typesense
npm install
cp .env.example .env
```

Then edit `.env` for your environment.

## Environment Variables

See `.env.example` for all options. Main variables:

```env
DB_HOST=127.0.0.1
DB_PORT=3306
DB_USER=root
DB_PASS=secret
DB_NAME=app

SYNC_CONFIG_PATH=config/sync.config.json
SYNC_BATCH_SIZE=1000

TS_NODE_HOST=127.0.0.1
TS_NODE_PORT=8108
TS_NODE_PROTOCOL=http
TS_API_KEY=xyz

CHECKPOINT_DRIVER=redis
REDIS_URL=redis://127.0.0.1:6379
CHECKPOINT_REDIS_KEY=mysql2typesense:binlog
CHECKPOINT_FILE=storage/checkpoints/binlog.json

MONITORING_ENABLED=true
MONITORING_HOST=0.0.0.0
MONITORING_PORT=8080
MONITORING_AUTH_TOKEN=

RETRY_MAX_ATTEMPTS=5
RETRY_BASE_DELAY_MS=500
LOG_LEVEL=info
```

## Sync Config

### Option A: Explicit tables mode

```json
{
  "tables": [
    {
      "database": "app",
      "table": "products",
      "primaryKey": "id",
      "collection": "products",
      "typesense": {
        "defaultSortingField": "updated_at_ts"
      }
    }
  ]
}
```

### Option B: Database auto mode

```json
{
  "database": {
    "name": "app",
    "excludeFields": ["password", "secret_token"],
    "infix_string": true,
    "json_stringify": ["IDs", "*IDs", "IDs*", "*IDs*"],
    "facet_fields": ["Status", "*Type*"]
  }
}
```

Behavior in database auto mode:

- All tables in `database.name` are synced.
- New tables discovered during runtime are automatically backfilled and added to realtime sync.
- Fields in `excludeFields` are removed from inferred schema/mapping.
- `infix_string: true` sets `infix: true` by default for inferred string fields.
- `json_stringify` marks matching columns as JSON-string source and converts them to Typesense `object` values.
- `facet_fields` marks matching inferred Typesense fields with `facet: true`.

Pattern rules for `json_stringify` and `facet_fields`:

- `IDs` = exact match
- `*IDs` = ends with `IDs`
- `IDs*` = starts with `IDs`
- `*IDs*` = contains `IDs`

Pattern matching is case-insensitive.

## Run Commands

Initial sync only:

```bash
npm run sync:initial
```

Realtime only:

```bash
npm run sync:realtime
```

Initial + realtime in one process:

```bash
npm run sync:bootstrap
```

Type check:

```bash
npm run typecheck
```

## Monitoring and Dashboard

- Dashboard: `http://<host>:8080/dashboard`
- Health: `GET /health`
- Metrics: `GET /metrics`
- Status: `GET /api/status`
- Collections: `GET /api/collections`
- Reindex: `POST /api/reindex/:name`
- Discovered tables: `GET /api/discovered-tables`

When `MONITORING_AUTH_TOKEN` is set, dashboard/admin endpoints require HTTP Basic Auth:

- Username: any value
- Password: value of `MONITORING_AUTH_TOKEN`

## Docker

Build locally:

```bash
docker build -t mysql2typesense .
```

Run container:

```bash
docker run -d --name mysql2typesense \
  -e DB_HOST=your_mysql_host \
  -e DB_PORT=3306 \
  -e DB_USER=your_mysql_user \
  -e DB_PASS=your_mysql_password \
  -e DB_NAME=your_mysql_database \
  -e SYNC_CONFIG_PATH=/app/config/sync.config.json \
  -e TS_NODE_HOST=your_typesense_host \
  -e TS_NODE_PORT=8108 \
  -e TS_NODE_PROTOCOL=http \
  -e TS_API_KEY=your_typesense_api_key \
  -e CHECKPOINT_DRIVER=redis \
  -e REDIS_URL=redis://your_redis_host:6379 \
  -e CHECKPOINT_REDIS_KEY=mysql2typesense:binlog \
  -e MONITORING_ENABLED=true \
  -e MONITORING_PORT=8080 \
  -p 8080:8080 \
  mysql2typesense
```

## Quick Deploy with Prebuilt Image (No Local Build)

You can deploy directly with prebuilt image `albertpham/mysql2typesense`.

Docker run:

```bash
docker run -d --name mysql2typesense \
  -e DB_HOST=your_mysql_host \
  -e DB_PORT=3306 \
  -e DB_USER=your_mysql_user \
  -e DB_PASS=your_mysql_password \
  -e DB_NAME=your_mysql_database \
  -e SYNC_CONFIG_PATH=/app/config/sync.config.json \
  -e TS_NODE_HOST=your_typesense_host \
  -e TS_NODE_PORT=8108 \
  -e TS_NODE_PROTOCOL=http \
  -e TS_API_KEY=your_typesense_api_key \
  -e CHECKPOINT_DRIVER=redis \
  -e REDIS_URL=redis://your_redis_host:6379 \
  -e CHECKPOINT_REDIS_KEY=mysql2typesense:binlog \
  -e MONITORING_ENABLED=true \
  -e MONITORING_PORT=8080 \
  -p 8080:8080 \
  albertpham/mysql2typesense:latest
```

Example compose service (image-based):

```yaml
services:
  sync-app:
    image: albertpham/mysql2typesense:latest
    restart: unless-stopped
    environment:
      DB_HOST: mysql
      DB_PORT: 3306
      DB_USER: sync_user
      DB_PASS: your_password
      DB_NAME: app
      SYNC_CONFIG_PATH: /app/config/sync.config.json
      TS_NODE_HOST: typesense
      TS_NODE_PORT: 8108
      TS_NODE_PROTOCOL: http
      TS_API_KEY: xyz
      CHECKPOINT_DRIVER: redis
      REDIS_URL: redis://redis:6379
      CHECKPOINT_REDIS_KEY: mysql2typesense:binlog
      MONITORING_ENABLED: "true"
      MONITORING_PORT: 8080
      MONITORING_AUTH_TOKEN: change_me
    ports:
      - "8080:8080"
    volumes:
      - ./config:/app/config:ro
```

## Local End-to-End Test (Docker Compose)

```bash
docker compose up --build
```

Sample stack includes seeded demo tables under `docker/mysql/init/001-schema.sql`.

## Notes

- Existing Typesense collections do not auto-change schema when you change inferred field options (for example `infix`). Recreate/reindex collection when needed.
- For production, use a dedicated MySQL user with least privileges instead of root.

## License

MIT
