import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { URL } from "node:url";

import type { Logger } from "pino";
import type { Client } from "typesense";

import type { SyncMonitor } from "../core/types.js";

export interface MonitoringServerOptions {
  host: string;
  port: number;
  logger: Logger;
  monitor: SyncMonitor;
  typesenseClient: Client;
}

function sendJson(response: ServerResponse, statusCode: number, payload: unknown) {
  response.statusCode = statusCode;
  response.setHeader("content-type", "application/json; charset=utf-8");
  response.end(JSON.stringify(payload));
}

function sendText(response: ServerResponse, statusCode: number, payload: string, contentType = "text/plain; charset=utf-8") {
  response.statusCode = statusCode;
  response.setHeader("content-type", contentType);
  response.end(payload);
}

async function handleApiCollections(
  request: IncomingMessage,
  response: ServerResponse,
  typesenseClient: Client
): Promise<boolean> {
  if (request.method === "GET" && request.url === "/api/collections") {
    const collections = await typesenseClient.collections().retrieve();
    sendJson(response, 200, { collections });
    return true;
  }

  if (request.method === "DELETE" && request.url?.startsWith("/api/collections/")) {
    const name = decodeURIComponent(request.url.replace("/api/collections/", ""));
    await typesenseClient.collections(name).delete();
    sendJson(response, 200, { ok: true, deleted: name });
    return true;
  }

  return false;
}

function dashboardHtml(): string {
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>MySQL2Typesense Dashboard</title>
  <style>
    :root {
      --bg: #f6f4ef;
      --card: #fffdf8;
      --line: #dad3c6;
      --text: #1b1b1b;
      --muted: #6a665f;
      --accent: #1f7a5a;
      --danger: #b42318;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Segoe UI", "Helvetica Neue", sans-serif;
      color: var(--text);
      background: radial-gradient(circle at 20% 10%, #fff 0%, var(--bg) 52%);
    }
    .wrap {
      max-width: 1080px;
      margin: 24px auto;
      padding: 0 16px;
      display: grid;
      gap: 16px;
    }
    .card {
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 16px;
    }
    h1 { margin: 0 0 6px; font-size: 28px; }
    h2 { margin: 0 0 10px; font-size: 18px; }
    .muted { color: var(--muted); }
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
      gap: 10px;
    }
    .metric {
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 10px;
    }
    .metric b { display: block; font-size: 20px; margin-top: 4px; }
    table { width: 100%; border-collapse: collapse; }
    th, td { text-align: left; padding: 8px; border-bottom: 1px solid var(--line); }
    button {
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #fff;
      padding: 6px 10px;
      cursor: pointer;
    }
    button:hover { border-color: var(--accent); }
    .danger { color: var(--danger); }
    .row { display: flex; align-items: center; justify-content: space-between; gap: 12px; }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <div class="row">
        <div>
          <h1>Sync Dashboard</h1>
          <div class="muted" id="statusLine">Loading...</div>
        </div>
        <button id="refreshBtn">Refresh</button>
      </div>
    </div>

    <div class="card">
      <h2>Metrics</h2>
      <div class="grid" id="metrics"></div>
    </div>

    <div class="card">
      <h2>Per Table</h2>
      <table>
        <thead>
          <tr><th>Table</th><th>Initial Docs</th><th>Upserts</th><th>Deletes</th></tr>
        </thead>
        <tbody id="tableStats"></tbody>
      </table>
    </div>

    <div class="card">
      <h2>Typesense Collections</h2>
      <table>
        <thead>
          <tr><th>Name</th><th>Documents</th><th>Action</th></tr>
        </thead>
        <tbody id="collections"></tbody>
      </table>
    </div>

    <div class="card">
      <h2>Recent Errors</h2>
      <table>
        <thead>
          <tr><th>Time</th><th>Context</th><th>Message</th></tr>
        </thead>
        <tbody id="errors"></tbody>
      </table>
    </div>
  </div>

  <script>
    async function fetchJson(url, options) {
      const response = await fetch(url, options);
      if (!response.ok) {
        throw new Error('Request failed: ' + response.status + ' ' + url);
      }
      return await response.json();
    }

    function renderMetrics(counters) {
      const root = document.getElementById('metrics');
      root.innerHTML = '';
      const entries = [
        ['Initial Batches', counters.initialBatches],
        ['Initial Documents', counters.initialDocuments],
        ['Realtime Upserts', counters.realtimeUpserts],
        ['Realtime Deletes', counters.realtimeDeletes],
        ['Errors', counters.errors]
      ];
      for (const [label, value] of entries) {
        const el = document.createElement('div');
        el.className = 'metric';
        el.innerHTML = '<span>' + label + '</span><b>' + value + '</b>';
        root.appendChild(el);
      }
    }

    function renderTableStats(perTable) {
      const body = document.getElementById('tableStats');
      body.innerHTML = '';
      Object.entries(perTable).forEach(([table, stats]) => {
        const tr = document.createElement('tr');
        tr.innerHTML = '<td>' + table + '</td><td>' + stats.initialDocuments + '</td><td>' + stats.upserts + '</td><td>' + stats.deletes + '</td>';
        body.appendChild(tr);
      });
    }

    async function renderCollections() {
      const body = document.getElementById('collections');
      body.innerHTML = '';
      const payload = await fetchJson('/api/collections');
      payload.collections.forEach((collection) => {
        const tr = document.createElement('tr');
        const btn = document.createElement('button');
        btn.className = 'danger';
        btn.textContent = 'Delete';
        btn.onclick = async () => {
          if (!confirm('Delete collection ' + collection.name + '?')) return;
          await fetchJson('/api/collections/' + encodeURIComponent(collection.name), { method: 'DELETE' });
          await refreshAll();
        };

        const actionTd = document.createElement('td');
        actionTd.appendChild(btn);
        tr.innerHTML = '<td>' + collection.name + '</td><td>' + collection.num_documents + '</td>';
        tr.appendChild(actionTd);
        body.appendChild(tr);
      });
    }

    function renderErrors(errors) {
      const body = document.getElementById('errors');
      body.innerHTML = '';
      errors.forEach((error) => {
        const tr = document.createElement('tr');
        tr.innerHTML = '<td>' + error.at + '</td><td>' + (error.context || '-') + '</td><td>' + error.message + '</td>';
        body.appendChild(tr);
      });
    }

    async function refreshAll() {
      const status = await fetchJson('/api/status');
      document.getElementById('statusLine').textContent =
        'Mode: ' + status.mode + ' | Started: ' + status.startedAt + ' | Tables: ' + status.tables.join(', ');
      renderMetrics(status.counters);
      renderTableStats(status.perTable);
      renderErrors(status.recentErrors);
      await renderCollections();
    }

    document.getElementById('refreshBtn').addEventListener('click', refreshAll);
    refreshAll();
    setInterval(refreshAll, 5000);
  </script>
</body>
</html>`;
}

export function startMonitoringServer(options: MonitoringServerOptions): Server {
  const { host, port, logger, monitor, typesenseClient } = options;

  const server = createServer(async (request, response) => {
    try {
      const url = new URL(request.url ?? "/", `http://${request.headers.host ?? "localhost"}`);

      if (request.method === "GET" && (url.pathname === "/" || url.pathname === "/dashboard")) {
        sendText(response, 200, dashboardHtml(), "text/html; charset=utf-8");
        return;
      }

      if (request.method === "GET" && url.pathname === "/health") {
        sendJson(response, 200, { ok: true, mode: monitor.snapshot().mode, at: new Date().toISOString() });
        return;
      }

      if (request.method === "GET" && url.pathname === "/metrics") {
        sendText(response, 200, monitor.toPrometheusMetrics());
        return;
      }

      if (request.method === "GET" && url.pathname === "/api/status") {
        sendJson(response, 200, monitor.snapshot());
        return;
      }

      if (await handleApiCollections(request, response, typesenseClient)) {
        return;
      }

      sendJson(response, 404, { error: "Not found" });
    } catch (error) {
      logger.error({ error }, "Monitoring request failed");
      sendJson(response, 500, { error: "Internal server error" });
    }
  });

  server.listen(port, host, () => {
    logger.info({ host, port }, "Monitoring server started");
  });

  return server;
}
