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
  authToken?: string;
  reindexCollection: (collectionName: string) => Promise<{ ok: boolean; reason?: string }>;
  updateCollectionSchema: (collectionName: string) => Promise<{ ok: boolean; reason?: string }>;
  resetTypesense: () => Promise<{ ok: boolean; reason?: string }>;
  getDiscoveredTables: () => {
    autoDiscoveryEnabled: boolean;
    startupDiscovered: string[];
    runtimeDiscovered: string[];
    currentTables: string[];
  };
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

function decodeBasicAuthToken(request: IncomingMessage): string | null {
  const authorization = request.headers.authorization;
  if (!authorization?.startsWith("Basic ")) {
    return null;
  }

  const encoded = authorization.slice("Basic ".length).trim();
  if (!encoded) {
    return null;
  }

  try {
    const raw = Buffer.from(encoded, "base64").toString("utf8");
    const [username, password = ""] = raw.split(":", 2);
    return password || username || null;
  } catch {
    return null;
  }
}

function isAuthorized(request: IncomingMessage, token?: string): boolean {
  if (!token) {
    return true;
  }

  return decodeBasicAuthToken(request) === token;
}

function challengeAuth(response: ServerResponse): void {
  response.statusCode = 401;
  response.setHeader("www-authenticate", 'Basic realm="mysql2typesense-dashboard"');
  response.end("Authentication required");
}

function isAdminRoute(pathname: string): boolean {
  return (
    pathname === "/dashboard" ||
    pathname === "/" ||
    pathname.startsWith("/api/collections") ||
    pathname.startsWith("/api/reindex") ||
    pathname.startsWith("/api/update-schema") ||
    pathname === "/api/reset" ||
    pathname === "/api/discovered-tables"
  );
}

async function handleApiCollections(
  request: IncomingMessage,
  url: URL,
  response: ServerResponse,
  typesenseClient: Client
): Promise<boolean> {
  if (request.method === "GET" && url.pathname === "/api/collections") {
    const collections = await typesenseClient.collections().retrieve();
    sendJson(response, 200, { collections });
    return true;
  }

  if (request.method === "DELETE" && url.pathname.startsWith("/api/collections/")) {
    const name = decodeURIComponent(url.pathname.replace("/api/collections/", ""));
    await typesenseClient.collections(name).delete();
    sendJson(response, 200, { ok: true, deleted: name });
    return true;
  }

  return false;
}

async function handleReindexRequest(
  request: IncomingMessage,
  url: URL,
  response: ServerResponse,
  reindexCollection: (collectionName: string) => Promise<{ ok: boolean; reason?: string }>
): Promise<boolean> {
  if (request.method !== "POST" || !url.pathname.startsWith("/api/reindex/")) {
    return false;
  }

  const collectionName = decodeURIComponent(url.pathname.replace("/api/reindex/", ""));
  if (!collectionName) {
    sendJson(response, 400, { ok: false, error: "Collection name is required" });
    return true;
  }

  const result = await reindexCollection(collectionName);
  if (result.ok) {
    sendJson(response, 200, { ok: true, collection: collectionName });
  } else {
    sendJson(response, 409, { ok: false, collection: collectionName, error: result.reason ?? "Reindex failed" });
  }

  return true;
}

async function handleUpdateSchemaRequest(
  request: IncomingMessage,
  url: URL,
  response: ServerResponse,
  updateCollectionSchema: (collectionName: string) => Promise<{ ok: boolean; reason?: string }>
): Promise<boolean> {
  if (request.method !== "POST" || !url.pathname.startsWith("/api/update-schema/")) {
    return false;
  }

  const collectionName = decodeURIComponent(url.pathname.replace("/api/update-schema/", ""));
  if (!collectionName) {
    sendJson(response, 400, { ok: false, error: "Collection name is required" });
    return true;
  }

  const result = await updateCollectionSchema(collectionName);
  if (result.ok) {
    sendJson(response, 200, { ok: true, collection: collectionName });
  } else {
    sendJson(response, 409, { ok: false, collection: collectionName, error: result.reason ?? "Update schema failed" });
  }

  return true;
}

async function handleResetRequest(
  request: IncomingMessage,
  url: URL,
  response: ServerResponse,
  resetTypesense: () => Promise<{ ok: boolean; reason?: string }>
): Promise<boolean> {
  if (request.method !== "POST" || url.pathname !== "/api/reset") {
    return false;
  }

  resetTypesense().catch(() => {
    // Error is recorded by reset implementation in monitor/logs.
  });
  sendJson(response, 202, { ok: true, message: "Reset started" });

  return true;
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
    .chart-wrap { width: 100%; height: 240px; }
    #throughputChart { width: 100%; height: 240px; border: 1px solid var(--line); border-radius: 8px; background: #fff; }
    .legend { display: flex; gap: 12px; margin-top: 8px; color: var(--muted); }
    .legend span::before { content: ""; display: inline-block; width: 12px; height: 12px; margin-right: 6px; border-radius: 2px; vertical-align: -1px; }
    .legend .upserts::before { background: #1f7a5a; }
    .legend .deletes::before { background: #b42318; }
    .muted-note { margin-top: 6px; color: var(--muted); font-size: 13px; }
    .pill {
      display: inline-flex;
      align-items: center;
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 4px 10px;
      font-size: 12px;
      margin-right: 6px;
      margin-bottom: 6px;
      background: #fff;
    }
    .pill.runtime {
      border-color: var(--accent);
      color: var(--accent);
      background: #edf8f3;
    }
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
        <div style="display:flex;gap:8px;">
          <button id="refreshBtn">Refresh</button>
        </div>
      </div>
    </div>

    <div class="card">
      <h2>Metrics</h2>
      <div class="grid" id="metrics"></div>
    </div>

    <div class="card">
      <h2>Realtime Throughput</h2>
      <div class="chart-wrap">
        <canvas id="throughputChart" width="1024" height="240"></canvas>
      </div>
      <div class="legend">
        <span class="upserts">Upserts / sec</span>
        <span class="deletes">Deletes / sec</span>
      </div>
      <div id="rateSummary" class="muted-note">Loading rates...</div>
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
      <h2>Auto-Discovered Tables</h2>
      <div class="muted-note" id="discoveryMode">Loading...</div>
      <div id="discoveredTables"></div>
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

    <div class="card" style="border-color:#b42318;">
      <h2 class="danger">Danger Zone</h2>
      <div class="row">
        <div>
          <b>Reset Typesense</b>
          <div class="muted-note">Xoa het du lieu Typesense, reset checkpoint binlog (Redis/file), dong bo lai tu dau.</div>
        </div>
        <button id="resetBtn" class="danger" style="border-color:#b42318;">Reset Typesense</button>
      </div>
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

    function drawThroughputChart(points) {
      const canvas = document.getElementById('throughputChart');
      const context = canvas.getContext('2d');
      if (!context) {
        return;
      }

      const width = canvas.width;
      const height = canvas.height;
      context.clearRect(0, 0, width, height);

      const padding = { top: 18, right: 16, bottom: 24, left: 36 };
      const plotWidth = width - padding.left - padding.right;
      const plotHeight = height - padding.top - padding.bottom;

      context.strokeStyle = '#dad3c6';
      context.lineWidth = 1;
      context.beginPath();
      context.moveTo(padding.left, padding.top);
      context.lineTo(padding.left, height - padding.bottom);
      context.lineTo(width - padding.right, height - padding.bottom);
      context.stroke();

      const normalized = points.slice(-60);
      const maxValue = Math.max(1, ...normalized.map((p) => Math.max(p.upserts, p.deletes)));
      const xStep = normalized.length > 1 ? plotWidth / (normalized.length - 1) : plotWidth;
      const toY = (value) => padding.top + plotHeight - (value / maxValue) * plotHeight;

      context.fillStyle = '#6a665f';
      context.font = '12px Segoe UI';
      context.fillText(String(maxValue), 6, padding.top + 4);
      context.fillText('0', 16, height - padding.bottom + 4);

      function drawLine(color, key) {
        context.strokeStyle = color;
        context.lineWidth = 2;
        context.beginPath();
        normalized.forEach((point, index) => {
          const x = padding.left + xStep * index;
          const y = toY(point[key]);
          if (index === 0) {
            context.moveTo(x, y);
          } else {
            context.lineTo(x, y);
          }
        });
        context.stroke();
      }

      if (normalized.length > 0) {
        drawLine('#1f7a5a', 'upserts');
        drawLine('#b42318', 'deletes');
      }
    }

    async function renderCollections() {
      const body = document.getElementById('collections');
      body.innerHTML = '';
      const payload = await fetchJson('/api/collections');
      payload.collections.forEach((collection) => {
        const tr = document.createElement('tr');
        const wrapper = document.createElement('div');
        wrapper.style.display = 'flex';
        wrapper.style.gap = '8px';

        const reindexBtn = document.createElement('button');
        reindexBtn.textContent = 'Reindex';
        reindexBtn.onclick = async () => {
          reindexBtn.disabled = true;
          try {
            await fetchJson('/api/reindex/' + encodeURIComponent(collection.name), { method: 'POST' });
            alert('Reindex started for ' + collection.name);
            await refreshAll();
          } catch (error) {
            alert('Reindex failed: ' + error.message);
          } finally {
            reindexBtn.disabled = false;
          }
        };

        const updateSchemaBtn = document.createElement('button');
        updateSchemaBtn.textContent = 'Force Update Schema';
        updateSchemaBtn.title = 'Cap nhat schema Typesense theo database moi nhat. Neu loi se xoa collection va dong bo lai.';
        updateSchemaBtn.onclick = async () => {
          updateSchemaBtn.disabled = true;
          updateSchemaBtn.textContent = 'Updating...';
          try {
            await fetchJson('/api/update-schema/' + encodeURIComponent(collection.name), { method: 'POST' });
            alert('Schema updated for ' + collection.name);
            await refreshAll();
          } catch (error) {
            alert('Update schema failed: ' + error.message);
          } finally {
            updateSchemaBtn.disabled = false;
            updateSchemaBtn.textContent = 'Force Update Schema';
          }
        };

        const btn = document.createElement('button');
        btn.className = 'danger';
        btn.textContent = 'Delete';
        btn.onclick = async () => {
          if (!confirm('Delete collection ' + collection.name + '?')) return;
          await fetchJson('/api/collections/' + encodeURIComponent(collection.name), { method: 'DELETE' });
          await refreshAll();
        };

        const actionTd = document.createElement('td');
        wrapper.appendChild(reindexBtn);
        wrapper.appendChild(updateSchemaBtn);
        wrapper.appendChild(btn);
        actionTd.appendChild(wrapper);
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

    async function renderDiscoveredTables() {
      const payload = await fetchJson('/api/discovered-tables');
      const modeLine = document.getElementById('discoveryMode');
      modeLine.textContent = payload.autoDiscoveryEnabled
        ? 'Auto-discovery is enabled (database mode).'
        : 'Auto-discovery is disabled (explicit tables config mode).';

      const root = document.getElementById('discoveredTables');
      root.innerHTML = '';

      const startupSet = new Set(payload.startupDiscovered || []);
      const runtimeSet = new Set(payload.runtimeDiscovered || []);
      const current = payload.currentTables || [];

      if (current.length === 0) {
        root.innerHTML = '<div class="muted-note">No tables are currently tracked.</div>';
        return;
      }

      current.forEach((tableName) => {
        const badge = document.createElement('span');
        const isRuntime = runtimeSet.has(tableName);
        badge.className = 'pill' + (isRuntime ? ' runtime' : '');
        const source = isRuntime ? 'runtime' : startupSet.has(tableName) ? 'startup' : 'tracked';
        badge.textContent = tableName + ' (' + source + ')';
        root.appendChild(badge);
      });
    }

    async function refreshAll() {
      const status = await fetchJson('/api/status');
      document.getElementById('statusLine').textContent =
        'Mode: ' + status.mode + ' | Started: ' + status.startedAt + ' | Tables: ' + status.tables.join(', ');
      renderMetrics(status.counters);
      renderTableStats(status.perTable);
      renderErrors(status.recentErrors);
      drawThroughputChart(status.throughput || []);
      const latest = (status.throughput || [])[Math.max(0, (status.throughput || []).length - 1)] || { upserts: 0, deletes: 0 };
      document.getElementById('rateSummary').textContent =
        'Current: ' + latest.upserts + ' upserts/s, ' + latest.deletes + ' deletes/s';
      await renderDiscoveredTables();
      await renderCollections();
    }

    document.getElementById('refreshBtn').addEventListener('click', refreshAll);

    document.getElementById('resetBtn').addEventListener('click', async () => {
      if (!confirm('CANH BAO: Thao tac nay se xoa TOAN BO du lieu Typesense va dong bo lai tu dau. Tiep tuc?')) return;
      if (!confirm('Xac nhan lan 2: Toan bo du lieu Typesense se bi xoa. Ban chac chan muon tiep tuc?')) return;
      const btn = document.getElementById('resetBtn');
      btn.disabled = true;
      btn.textContent = 'Resetting...';
      try {
        await fetchJson('/api/reset', { method: 'POST' });
        alert('Reset da bat dau. He thong dang dong bo lai du lieu trong nen.');
        await refreshAll();
      } catch (error) {
        alert('Reset that bai: ' + error.message);
      } finally {
        btn.disabled = false;
        btn.textContent = 'Reset Typesense';
      }
    });

    refreshAll();
  </script>
</body>
</html>`;
}

export function startMonitoringServer(options: MonitoringServerOptions): Server {
  const { host, port, logger, monitor, typesenseClient, authToken, reindexCollection, updateCollectionSchema, resetTypesense, getDiscoveredTables } = options;

  const server = createServer(async (request, response) => {
    try {
      const url = new URL(request.url ?? "/", `http://${request.headers.host ?? "localhost"}`);

      if (isAdminRoute(url.pathname) && !isAuthorized(request, authToken)) {
        challengeAuth(response);
        return;
      }

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

      if (request.method === "GET" && url.pathname === "/api/discovered-tables") {
        sendJson(response, 200, getDiscoveredTables());
        return;
      }

      if (await handleApiCollections(request, url, response, typesenseClient)) {
        return;
      }

      if (await handleReindexRequest(request, url, response, reindexCollection)) {
        return;
      }

      if (await handleUpdateSchemaRequest(request, url, response, updateCollectionSchema)) {
        return;
      }

      if (await handleResetRequest(request, url, response, resetTypesense)) {
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
