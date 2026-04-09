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
      --bg: #f2f5f7;
      --card: #ffffff;
      --line: #d6dde3;
      --text: #18212a;
      --muted: #5d6773;
      --accent: #0b6b7a;
      --accent-soft: #e6f4f7;
      --danger: #b42318;
      --sidebar: #0f1722;
      --sidebar-text: #d7e2ee;
      --sidebar-active: #16485e;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Segoe UI", "Helvetica Neue", sans-serif;
      color: var(--text);
      background:
        radial-gradient(circle at 10% 20%, #ffffff 0%, transparent 35%),
        radial-gradient(circle at 90% 10%, #e8eef2 0%, transparent 30%),
        linear-gradient(180deg, #eef2f5 0%, var(--bg) 100%);
    }
    .layout {
      max-width: 1320px;
      margin: 20px auto;
      padding: 0 16px;
      display: grid;
      grid-template-columns: 260px minmax(0, 1fr);
      gap: 16px;
      align-items: start;
    }
    .sidebar {
      position: sticky;
      top: 16px;
      background: var(--sidebar);
      color: var(--sidebar-text);
      border-radius: 14px;
      border: 1px solid #223246;
      padding: 14px;
      max-height: calc(100vh - 32px);
      overflow: auto;
    }
    .brand {
      font-size: 17px;
      font-weight: 700;
      margin: 4px 2px 14px;
      color: #fff;
    }
    .sidebar-state {
      border: 1px solid #2b3d52;
      border-radius: 10px;
      padding: 10px;
      margin: 0 0 10px;
      background: rgba(255, 255, 255, 0.03);
      color: var(--sidebar-text);
      font-size: 12px;
    }
    .mode-pill {
      display: inline-block;
      border: 1px solid #37516c;
      border-radius: 999px;
      padding: 3px 8px;
      font-weight: 600;
      margin-bottom: 6px;
      text-transform: uppercase;
      letter-spacing: 0.4px;
    }
    .mode-pill.idle {
      background: #1e2b39;
      border-color: #37516c;
    }
    .mode-pill.initial {
      background: #4d3c12;
      border-color: #9f7a1f;
      color: #ffedbf;
    }
    .mode-pill.realtime {
      background: #0f3f2a;
      border-color: #1f7a5a;
      color: #b7f2da;
    }
    .menu {
      display: grid;
      gap: 8px;
    }
    .menu-item {
      width: 100%;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      text-align: left;
      border: 1px solid #2b3d52;
      background: transparent;
      color: var(--sidebar-text);
      border-radius: 10px;
      padding: 10px;
      cursor: pointer;
      transition: background-color .2s ease, border-color .2s ease;
    }
    .menu-item:hover {
      border-color: #3a5673;
      background: rgba(255, 255, 255, 0.05);
    }
    .menu-item.active {
      background: var(--sidebar-active);
      border-color: #4f94bc;
      color: #fff;
    }
    .menu-error-badge {
      min-width: 22px;
      padding: 1px 7px;
      border-radius: 999px;
      background: #3b4f64;
      color: #fff;
      font-size: 11px;
      text-align: center;
      font-weight: 700;
    }
    .menu-error-badge.alert {
      background: #b42318;
    }
    .content {
      display: grid;
      gap: 16px;
    }
    .card {
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 16px;
      scroll-margin-top: 12px;
    }
    #section-overview {
      position: sticky;
      top: 12px;
      z-index: 7;
      backdrop-filter: blur(4px);
      background: rgba(255, 255, 255, 0.92);
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
    tr.clickable { cursor: pointer; }
    tr.clickable:hover td { background: var(--accent-soft); }
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
      background: var(--accent-soft);
    }
    @media (max-width: 980px) {
      .layout {
        grid-template-columns: 1fr;
      }
      .sidebar {
        position: static;
        max-height: none;
      }
      .menu {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }
      #section-overview {
        position: static;
      }
    }
    @media (max-width: 640px) {
      .menu {
        grid-template-columns: 1fr;
      }
    }
    /* Error detail modal */
    .modal-backdrop {
      display: none;
      position: fixed;
      inset: 0;
      z-index: 100;
      background: rgba(15, 23, 34, 0.65);
      backdrop-filter: blur(3px);
      align-items: center;
      justify-content: center;
    }
    .modal-backdrop.open { display: flex; }
    .modal {
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 24px;
      max-width: 860px;
      width: calc(100vw - 32px);
      max-height: 90vh;
      overflow-y: auto;
      box-shadow: 0 24px 64px rgba(0,0,0,0.25);
    }
    .modal-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 16px;
    }
    .modal-header h2 { margin: 0; font-size: 17px; }
    .modal-close {
      border: none;
      background: transparent;
      font-size: 20px;
      cursor: pointer;
      color: var(--muted);
      padding: 4px 8px;
      border-radius: 6px;
    }
    .modal-close:hover { background: var(--line); color: var(--text); }
    .modal-field { margin-bottom: 14px; }
    .modal-label {
      font-size: 11px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.6px;
      color: var(--muted);
      margin-bottom: 4px;
    }
    .modal-value {
      font-size: 13px;
      word-break: break-word;
    }
    .modal-pre {
      background: #f6f8fa;
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 12px;
      font-family: "SF Mono", "Cascadia Code", "Consolas", monospace;
      font-size: 12px;
      white-space: pre-wrap;
      word-break: break-all;
      max-height: 380px;
      overflow-y: auto;
      margin: 0;
    }
    .modal-pre .json-key { color: #0550ae; }
    .modal-pre .json-str { color: #116329; }
    .modal-pre .json-num { color: #953800; }
    .modal-pre .json-bool { color: #8250df; }
    .modal-pre .json-null { color: #b42318; }
    .modal-actions { display: flex; gap: 8px; margin-top: 16px; justify-content: flex-end; }
    .error-hint { font-size: 12px; color: var(--muted); margin-bottom: 8px; }
  </style>
</head>
<body>
  <!-- Error detail modal -->
  <div class="modal-backdrop" id="errorModal" role="dialog" aria-modal="true" aria-label="Error Detail">
    <div class="modal">
      <div class="modal-header">
        <h2>Error Detail</h2>
        <button class="modal-close" id="modalCloseBtn" aria-label="Close">&times;</button>
      </div>
      <div class="modal-field">
        <div class="modal-label">Time</div>
        <div class="modal-value" id="modalAt"></div>
      </div>
      <div class="modal-field">
        <div class="modal-label">Context</div>
        <div class="modal-value" id="modalContext"></div>
        <div id="modalContextDetail" style="margin-top:8px;display:flex;flex-wrap:wrap;gap:6px;"></div>
      </div>
      <div class="modal-field" id="modalAnalysisField">
        <div class="modal-label">Error Analysis &amp; Fix Suggestion</div>
        <div id="modalAnalysis" style="font-size:13px;line-height:1.75;border:1px solid #fde8e8;border-radius:8px;padding:12px 14px;background:#fff8f8;"></div>
      </div>
      <div class="modal-field">
        <div class="modal-label">Error Message</div>
        <pre class="modal-pre" id="modalMessage" style="color:var(--danger);"></pre>
      </div>
      <div class="modal-field" id="modalDataField">
        <div class="modal-label">Document Data <span style="font-weight:400;color:var(--muted);">(highlighted field = likely cause)</span></div>
        <pre class="modal-pre" id="modalData"></pre>
      </div>
      <div class="modal-actions">
        <button id="modalCopyReportBtn" style="background:var(--accent);color:#fff;border-color:var(--accent);font-weight:600;">Copy Full Report</button>
        <button id="modalCopyBtn">Copy JSON</button>
        <button id="modalCloseBtnBottom">Close</button>
      </div>
    </div>
  </div>
  <div class="layout">
    <aside class="sidebar">
      <div class="brand">Sync Dashboard</div>
      <div class="sidebar-state">
        <div id="modePill" class="mode-pill idle">IDLE</div>
        <div id="menuModeHint">Current mode: idle</div>
      </div>
      <div class="menu" id="sideMenu">
        <button class="menu-item active" data-target="section-overview">Overview</button>
        <button class="menu-item" data-target="section-metrics">Metrics</button>
        <button class="menu-item" data-target="section-throughput">Realtime Throughput</button>
        <button class="menu-item" data-target="section-per-table">Per Table</button>
        <button class="menu-item" data-target="section-discovered">Discovered Tables</button>
        <button class="menu-item" data-target="section-collections">Collections</button>
        <button class="menu-item" data-target="section-errors">
          <span>Recent Errors</span>
          <span id="menuErrorCount" class="menu-error-badge">0</span>
        </button>
        <button class="menu-item" data-target="section-danger">Danger Zone</button>
      </div>
    </aside>

    <main class="content">
      <section class="card" id="section-overview">
        <div class="row">
          <div>
            <h1>Sync Dashboard</h1>
            <div class="muted" id="statusLine">Loading...</div>
          </div>
          <div style="display:flex;gap:8px;">
            <button id="refreshBtn">Refresh</button>
          </div>
        </div>
      </section>

      <section class="card" id="section-metrics">
        <h2>Metrics</h2>
        <div class="grid" id="metrics"></div>
      </section>

      <section class="card" id="section-throughput">
        <h2>Realtime Throughput</h2>
        <div class="chart-wrap">
          <canvas id="throughputChart" width="1024" height="240"></canvas>
        </div>
        <div class="legend">
          <span class="upserts">Upserts / sec</span>
          <span class="deletes">Deletes / sec</span>
        </div>
        <div id="rateSummary" class="muted-note">Loading rates...</div>
      </section>

      <section class="card" id="section-per-table">
        <h2>Per Table</h2>
        <table>
          <thead>
            <tr><th>Table</th><th>Initial Docs</th><th>Upserts</th><th>Deletes</th></tr>
          </thead>
          <tbody id="tableStats"></tbody>
        </table>
      </section>

      <section class="card" id="section-discovered">
        <h2>Auto-Discovered Tables</h2>
        <div class="muted-note" id="discoveryMode">Loading...</div>
        <div id="discoveredTables"></div>
      </section>

      <section class="card" id="section-collections">
        <h2>Typesense Collections</h2>
        <table>
          <thead>
            <tr><th>Name</th><th>Documents</th><th>Action</th></tr>
          </thead>
          <tbody id="collections"></tbody>
        </table>
      </section>

      <section class="card" id="section-errors">
        <h2>Recent Errors</h2>
        <div class="error-hint">Click a row to see full error detail and document data.</div>
        <table>
          <thead>
            <tr><th>Time</th><th>Context</th><th>Message</th></tr>
          </thead>
          <tbody id="errors"></tbody>
        </table>
      </section>

      <section class="card" id="section-danger" style="border-color:#b42318;">
        <h2 class="danger">Danger Zone</h2>
        <div class="row">
          <div>
            <b>Reset Typesense</b>
            <div class="muted-note">Remove all Typesense data, reset checkpoint binlog (Redis/file), and resynchronize from the beginning.</div>
          </div>
          <button id="resetBtn" class="danger" style="border-color:#b42318;">Reset Typesense</button>
        </div>
      </section>
    </main>
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

    function syntaxHighlightJson(json) {
      return json
        .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
        .replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g, (match) => {
          if (/^"/.test(match)) {
            return /:$/.test(match)
              ? '<span class="json-key">' + match + '</span>'
              : '<span class="json-str">' + match + '</span>';
          }
          if (/true|false/.test(match)) return '<span class="json-bool">' + match + '</span>';
          if (/null/.test(match)) return '<span class="json-null">' + match + '</span>';
          return '<span class="json-num">' + match + '</span>';
        });
    }

    function parseContext(ctx) {
      if (!ctx) return {};
      // Formats: "initial:db.Table:document:id", "realtime:db.Table", "reindex:Collection", "reset", "auto-discovery:key"
      const parts = ctx.split(':');
      const op = parts[0];
      const result = { op };
      if (op === 'initial' || op === 'realtime') {
        const tableKey = parts[1] || '';
        const dotIdx = tableKey.indexOf('.');
        result.database = dotIdx !== -1 ? tableKey.slice(0, dotIdx) : tableKey;
        result.table = dotIdx !== -1 ? tableKey.slice(dotIdx + 1) : '';
        if (parts[2] === 'document') result.documentId = parts[3];
      } else if (op === 'reindex' || op === 'update-schema') {
        result.collection = parts.slice(1).join(':');
      } else if (op === 'auto-discovery') {
        result.key = parts.slice(1).join(':');
      }
      return result;
    }

    let currentError = null;

    function openErrorModal(error) {
      currentError = error;
      const ctx = parseContext(error.context);

      document.getElementById('modalAt').textContent = error.at;
      document.getElementById('modalContext').textContent = error.context || '—';
      document.getElementById('modalMessage').textContent = error.message;

      // Context breakdown chips
      const detail = document.getElementById('modalContextDetail');
      detail.innerHTML = '';
      const chipBase = 'display:inline-flex;align-items:center;border-radius:6px;padding:3px 9px;font-size:12px;font-weight:600;border:1px solid;';
      function addChip(label, value, bg, color) {
        const chip = document.createElement('span');
        chip.style.cssText = chipBase + 'background:' + bg + ';color:' + color + ';border-color:' + color + '44;';
        chip.innerHTML = (label ? '<span style="opacity:0.6;font-weight:400;margin-right:3px;">' + label + '</span>' : '') + value;
        detail.appendChild(chip);
      }
      const opMap = {
        initial: ['Initial Sync', '#e6f4f7', '#0b6b7a'],
        realtime: ['Realtime', '#e7f5ef', '#1f7a5a'],
        reindex: ['Reindex', '#fef9ec', '#9f7a1f'],
        'update-schema': ['Update Schema', '#f4ecfe', '#7a3f9f'],
        reset: ['Reset', '#fde8e8', '#b42318'],
        'auto-discovery': ['Auto-Discovery', '#f2f5f7', '#5d6773']
      };
      const opInfo = opMap[ctx.op] || [ctx.op, '#f2f5f7', '#5d6773'];
      if (ctx.op) addChip('', opInfo[0], opInfo[1], opInfo[2]);
      if (ctx.database) addChip('DB:', ctx.database, '#e8f0fe', '#0550ae');
      if (ctx.table) addChip('Table:', ctx.table, '#e8f5ec', '#116329');
      if (ctx.documentId) addChip('Doc ID:', ctx.documentId, '#fff3e8', '#953800');
      if (ctx.collection) addChip('Collection:', ctx.collection, '#e6f4f7', '#0b6b7a');

      // Error analysis with fix suggestions
      const analysisField = document.getElementById('modalAnalysisField');
      const analysis = document.getElementById('modalAnalysis');
      const msg = error.message;
      const rows = [];
      let highlightField = null;

      const refMatch = msg.match(new RegExp('Referenced field [\\x60\'?]?(\\w+)[\\x60\'?]? not found in the collection [\\x60\'?]?(\\w+)[\\x60\'?]?', 'i'));
      if (refMatch) {
        highlightField = refMatch[1];
        rows.push('&#10060; Field <strong>' + refMatch[1] + '</strong> được dùng làm join reference target trong collection <strong>' + refMatch[2] + '</strong>, nhưng field này không tồn tại hoặc là optional trong collection đó.');
        rows.push('&#128161; Fix: Reindex collection <strong>' + refMatch[2] + '</strong> trước, rồi mới reindex <strong>' + (ctx.table || 'table này') + '</strong>. Nếu vẫn lỗi, kiểm tra <code>join_configs</code> đúng field name và type chưa.');
      }

      const typeMatch = msg.match(new RegExp('Field [\\x60\'?]?(\\w+)[\\x60\'?]? must have ([\\w]+) value', 'i'));
      if (typeMatch) {
        highlightField = highlightField || typeMatch[1];
        rows.push('&#10060; Field <strong>' + typeMatch[1] + '</strong> gửi sai kiểu dữ liệu — Typesense yêu cầu <strong>' + typeMatch[2] + '</strong>.');
        rows.push('&#128161; Fix: Trong <code>join_configs</code> của table này, thêm <code>&quot;type&quot;: &quot;' + typeMatch[2] + '&quot;</code> cho field <strong>' + typeMatch[1] + '</strong>.');
      }

      const notFoundDoc = msg.match(new RegExp('Field [\\x60\'?]?(\\w+)[\\x60\'?]? not found in the document', 'i'));
      if (notFoundDoc) {
        highlightField = highlightField || notFoundDoc[1];
        rows.push('&#9888; Field <strong>' + notFoundDoc[1] + '</strong> bắt buộc nhưng không có trong document (thường do binlog partial event khi MySQL binlog-row-image != FULL).');
        rows.push('&#128161; Fix: Đánh dấu field <strong>' + notFoundDoc[1] + '</strong> là <code>&quot;optional&quot;: true</code> trong config.');
      }

      const importFailed = msg.match(/Typesense import failed for document (\S+): (.+)/i);
      if (!refMatch && !typeMatch && !notFoundDoc && importFailed) {
        rows.push('&#10060; Import thất bại cho document <strong>' + importFailed[1] + '</strong>.');
        rows.push('&#128161; Kiểm tra Typesense schema của collection <strong>' + (ctx.table || '?') + '</strong> và đối chiếu với document data bên dưới.');
      }

      if (rows.length > 0) {
        analysis.innerHTML = rows.map(r => '<div style="padding:2px 0;">' + r + '</div>').join('');
        analysisField.style.display = '';
      } else {
        analysisField.style.display = 'none';
      }

      // Document data with syntax highlight + highlight the problematic field
      const dataField = document.getElementById('modalDataField');
      const dataEl = document.getElementById('modalData');
      if (error.data) {
        const formatted = JSON.stringify(error.data, null, 2);
        let html = syntaxHighlightJson(formatted);
        if (highlightField) {
          // Wrap the key span of the problematic field in a highlight mark
          html = html.replace(
            new RegExp('(<span class="json-key">"' + highlightField + '"<\/span>)(: )(<span[^>]*>[^<]*<\/span>)', 'g'),
            '<mark style="background:#fff3cd;border-radius:3px;outline:2px solid #e6a817;outline-offset:1px;">$1$2$3</mark>'
          );
        }
        dataEl.innerHTML = html;
        dataField.style.display = '';
      } else {
        dataField.style.display = 'none';
      }

      document.getElementById('errorModal').classList.add('open');
    }

    function closeErrorModal() {
      document.getElementById('errorModal').classList.remove('open');
    }

    document.getElementById('modalCloseBtn').addEventListener('click', closeErrorModal);
    document.getElementById('modalCloseBtnBottom').addEventListener('click', closeErrorModal);
    document.getElementById('errorModal').addEventListener('click', (e) => {
      if (e.target === document.getElementById('errorModal')) closeErrorModal();
    });
    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape') closeErrorModal();
    });
    document.getElementById('modalCopyReportBtn').addEventListener('click', async () => {
      if (!currentError) return;
      const ctx = parseContext(currentError.context);
      const lines = ['## Bug Report — mysql2typesense', ''];
      lines.push('**Time:** ' + currentError.at);
      lines.push('**Context:** ' + (currentError.context || '—'));
      if (ctx.op) lines.push('**Operation:** ' + ctx.op);
      if (ctx.database && ctx.table) lines.push('**Table:** ' + ctx.database + '.' + ctx.table);
      if (ctx.documentId) lines.push('**Document ID:** ' + ctx.documentId);
      if (ctx.collection) lines.push('**Collection:** ' + ctx.collection);
      lines.push('');
      lines.push('**Error:**');
      lines.push('\`\`\`');
      lines.push(currentError.message);
      lines.push('\`\`\`');
      if (currentError.data) {
        lines.push('');
        lines.push('**Document Data:**');
        lines.push('\`\`\`json');
        lines.push(JSON.stringify(currentError.data, null, 2));
        lines.push('\`\`\`');
      }
      try {
        await navigator.clipboard.writeText(lines.join('\n'));
        const btn = document.getElementById('modalCopyReportBtn');
        btn.textContent = 'Copied!';
        setTimeout(() => { btn.textContent = 'Copy Full Report'; }, 2500);
      } catch {}
    });
    document.getElementById('modalCopyBtn').addEventListener('click', async () => {
      if (!currentError?.data) return;
      try {
        await navigator.clipboard.writeText(JSON.stringify(currentError.data, null, 2));
        const btn = document.getElementById('modalCopyBtn');
        btn.textContent = 'Copied!';
        setTimeout(() => { btn.textContent = 'Copy JSON'; }, 2000);
      } catch {}
    });

    function renderErrors(errors) {
      const body = document.getElementById('errors');
      body.innerHTML = '';
      if (errors.length === 0) {
        const tr = document.createElement('tr');
        tr.innerHTML = '<td colspan="3" class="muted" style="text-align:center;padding:16px;">No errors recorded.</td>';
        body.appendChild(tr);
        return;
      }
      errors.forEach((error) => {
        const tr = document.createElement('tr');
        tr.className = 'clickable';
        tr.title = 'Click to see error detail' + (error.data ? ' and document data' : '');
        const time = document.createElement('td');
        time.style.whiteSpace = 'nowrap';
        time.textContent = error.at;
        const ctx = document.createElement('td');
        ctx.style.whiteSpace = 'nowrap';
        ctx.textContent = error.context || '—';
        const msg = document.createElement('td');
        msg.style.color = 'var(--danger)';
        msg.textContent = error.message.length > 120 ? error.message.slice(0, 120) + '…' : error.message;
        if (error.data) {
          const badge = document.createElement('span');
          badge.style.cssText = 'margin-left:6px;font-size:10px;background:#e6f4f7;color:#0b6b7a;border:1px solid #0b6b7a;border-radius:4px;padding:1px 5px;vertical-align:middle;';
          badge.textContent = 'data';
          msg.appendChild(badge);
        }
        tr.appendChild(time);
        tr.appendChild(ctx);
        tr.appendChild(msg);
        tr.addEventListener('click', () => openErrorModal(error));
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
      if (refreshAll.running) {
        return;
      }
      refreshAll.running = true;
      try {
        const status = await fetchJson('/api/status');
        document.getElementById('statusLine').textContent =
          'Mode: ' + status.mode + ' | Started: ' + status.startedAt + ' | Tables: ' + status.tables.length;

        const modePill = document.getElementById('modePill');
        const modeHint = document.getElementById('menuModeHint');
        modePill.className = 'mode-pill ' + status.mode;
        modePill.textContent = String(status.mode || 'idle').toUpperCase();
        modeHint.textContent = 'Current mode: ' + status.mode;

        const errorCount = Number(status?.counters?.errors || 0);
        const menuErrorCount = document.getElementById('menuErrorCount');
        menuErrorCount.textContent = String(errorCount);
        menuErrorCount.classList.toggle('alert', errorCount > 0);

        renderMetrics(status.counters);
        renderTableStats(status.perTable);
        renderErrors(status.recentErrors);
        drawThroughputChart(status.throughput || []);
        const latest = (status.throughput || [])[Math.max(0, (status.throughput || []).length - 1)] || { upserts: 0, deletes: 0 };
        document.getElementById('rateSummary').textContent =
          'Current: ' + latest.upserts + ' upserts/s, ' + latest.deletes + ' deletes/s';
        await renderDiscoveredTables();
        await renderCollections();
      } finally {
        refreshAll.running = false;
      }
    }
    refreshAll.running = false;

    document.getElementById('refreshBtn').addEventListener('click', refreshAll);

    const menuItems = Array.from(document.querySelectorAll('#sideMenu .menu-item'));
    const sectionIds = menuItems.map((item) => item.getAttribute('data-target')).filter(Boolean);
    const sections = sectionIds
      .map((id) => document.getElementById(id))
      .filter(Boolean);

    function setActiveMenu(targetId) {
      menuItems.forEach((item) => {
        item.classList.toggle('active', item.getAttribute('data-target') === targetId);
      });
    }

    menuItems.forEach((item) => {
      item.addEventListener('click', () => {
        const targetId = item.getAttribute('data-target');
        if (!targetId) return;
        const target = document.getElementById(targetId);
        if (!target) return;
        target.scrollIntoView({ behavior: 'smooth', block: 'start' });
        setActiveMenu(targetId);
      });
    });

    window.addEventListener('scroll', () => {
      let activeId = sectionIds[0] || '';
      for (const section of sections) {
        const rect = section.getBoundingClientRect();
        if (rect.top <= 120) {
          activeId = section.id;
        }
      }
      if (activeId) {
        setActiveMenu(activeId);
      }
    }, { passive: true });

    document.getElementById('resetBtn').addEventListener('click', async () => {
      if (!confirm('WARNING: This action will delete ALL Typesense data and reindex from scratch. Continue?')) return;
      if (!confirm('Confirm again: All Typesense data will be deleted. Are you sure you want to proceed?')) return;
      const btn = document.getElementById('resetBtn');
      btn.disabled = true;
      btn.textContent = 'Resetting...';
      try {
        await fetchJson('/api/reset', { method: 'POST' });
        alert('Reset has started. The system is reindexing data in the background.');
        await refreshAll();
      } catch (error) {
        alert('Reset failed: ' + error.message);
      } finally {
        btn.disabled = false;
        btn.textContent = 'Reset Typesense';
      }
    });

    refreshAll();
    setInterval(() => {
      refreshAll().catch(() => {
        // Errors are already visible in UI via API responses and recent errors table.
      });
    }, 1000 * 30);
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
