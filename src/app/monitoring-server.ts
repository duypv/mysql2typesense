import { readFileSync } from "node:fs";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { join } from "node:path";
import { URL } from "node:url";

import type { Logger } from "pino";
import type { Client } from "typesense";

import type { JoinReferenceDiagnosticsReport, ResetStatusSnapshot, SyncMonitor } from "../core/types.js";

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
  getJoinReferenceDiagnostics: () => Promise<JoinReferenceDiagnosticsReport>;
  getResetStatus: () => ResetStatusSnapshot;
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
    pathname === "/api/reset-status" ||
    pathname === "/api/join-integrity" ||
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

  if (request.method === "GET") {
    const segments = url.pathname.split("/").filter(Boolean);
    if (segments.length === 4 && segments[0] === "api" && segments[1] === "collections" && segments[3] === "documents") {
      const collectionName = decodeURIComponent(segments[2]);
      const pageParam = Number.parseInt(url.searchParams.get("page") ?? "1", 10);
      const perPageParam = Number.parseInt(url.searchParams.get("perPage") ?? "50", 10);
      const page = Number.isNaN(pageParam) || pageParam < 1 ? 1 : pageParam;
      const perPage = Number.isNaN(perPageParam) || perPageParam < 1 ? 50 : Math.min(perPageParam, 200);

      const exportedDocuments = await typesenseClient.collections(collectionName).documents().export();
      const lines = exportedDocuments
        .split("\n")
        .map((line) => line.trim())
        .filter((line) => line.length > 0);

      const totalDocuments = lines.length;
      const totalPages = totalDocuments === 0 ? 1 : Math.ceil(totalDocuments / perPage);
      const safePage = Math.min(page, totalPages);
      const start = (safePage - 1) * perPage;
      const pageItems = lines.slice(start, start + perPage).map((line) => JSON.parse(line) as Record<string, unknown>);

      sendJson(response, 200, {
        collection: collectionName,
        page: safePage,
        perPage,
        totalDocuments,
        totalPages,
        items: pageItems
      });
      return true;
    }
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
  return readFileSync(join(__dirname, "dashboard.html"), "utf8");
}

export function startMonitoringServer(options: MonitoringServerOptions): Server {
  const {
    host,
    port,
    logger,
    monitor,
    typesenseClient,
    authToken,
    reindexCollection,
    updateCollectionSchema,
    resetTypesense,
    getJoinReferenceDiagnostics,
    getResetStatus,
    getDiscoveredTables
  } = options;

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

      if (request.method === "GET" && url.pathname === "/api/join-integrity") {
        const diagnostics = await getJoinReferenceDiagnostics();
        sendJson(response, 200, diagnostics);
        return;
      }

      if (request.method === "GET" && url.pathname === "/api/reset-status") {
        sendJson(response, 200, getResetStatus());
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
