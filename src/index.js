import { DurableObject } from "cloudflare:workers";

function packFrame(headerObj, payloadBuf) {
  const headerBytes = new TextEncoder().encode(JSON.stringify(headerObj));
  const payload = payloadBuf ? new Uint8Array(payloadBuf) : new Uint8Array(0);
  const out = new Uint8Array(4 + headerBytes.length + payload.length);
  new DataView(out.buffer).setUint32(0, headerBytes.length, false);
  out.set(headerBytes, 4);
  out.set(payload, 4 + headerBytes.length);
  return out;
}

function unpackFrame(arrayBuf) {
  const view = new DataView(arrayBuf);
  const headerLen = view.getUint32(0, false);
  const headerJson = new TextDecoder().decode(new Uint8Array(arrayBuf, 4, headerLen));
  const payload = arrayBuf.slice(4 + headerLen);
  return { header: JSON.parse(headerJson), payload };
}

export class RmbgRelay extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS results (
        job_id TEXT PRIMARY KEY,
        ok INTEGER NOT NULL,
        error TEXT,
        mask BLOB,
        created_at INTEGER NOT NULL
      )
    `);
    this.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
  }

  async fetch(request) {
    const url = new URL(request.url);

    // 1. Streamlit worker connects here via WebSocket
    if (url.pathname === "/ws") {
      if (request.headers.get("Upgrade") !== "websocket") {
        return new Response("Expected WebSocket", { status: 426 });
      }
      // Replace any stale worker socket so only 1 active worker receives jobs
      for (const oldWs of this.ctx.getWebSockets("worker")) {
        try {
          oldWs.close(1000, "replaced");
        } catch {}
      }
      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      this.ctx.acceptWebSocket(server, ["worker"]);
      return new Response(null, { status: 101, webSocket: client });
    }

    // 2. rb.py checks if worker is online
    if (url.pathname === "/health") {
      const workers = this.ctx.getWebSockets("worker");
      return Response.json({ worker_online: workers.length > 0 });
    }

    // 3. rb.py submits an image job
    if (url.pathname === "/job" && request.method === "POST") {
      const jobId = url.searchParams.get("id");
      const ext = url.searchParams.get("ext") || "webp";
      if (!jobId) return new Response("Missing id", { status: 400 });

      const workers = this.ctx.getWebSockets("worker");
      if (workers.length === 0) {
        return new Response(
          "Worker offline: open your Streamlit app URL in a browser to wake it.",
          { status: 503 }
        );
      }

      // Clean results older than 10 minutes
      const cutoff = Date.now() - 10 * 60 * 1000;
      this.ctx.storage.sql.exec("DELETE FROM results WHERE created_at < ?", cutoff);

      const imgBuf = await request.arrayBuffer();
      const frame = packFrame({ type: "req", job_id: jobId, ext }, imgBuf);

      try {
        workers[0].send(frame);
      } catch (err) {
        try {
          workers[0].close(1011, "send failed");
        } catch {}
        return new Response("Worker disconnected; retry shortly.", { status: 503 });
      }

      return Response.json({ status: "queued", job_id: jobId }, { status: 202 });
    }

    // 4. rb.py polls for finished mask
    if (url.pathname === "/result" && request.method === "GET") {
      const jobId = url.searchParams.get("id");
      if (!jobId) return new Response("Missing id", { status: 400 });

      const rows = this.ctx.storage.sql
        .exec("SELECT ok, error, mask FROM results WHERE job_id = ?", jobId)
        .toArray();

      if (rows.length === 0) {
        return new Response("pending", { status: 202 });
      }

      const row = rows[0];
      this.ctx.storage.sql.exec("DELETE FROM results WHERE job_id = ?", jobId);

      if (!row.ok) {
        return new Response(row.error || "Unknown worker error", { status: 500 });
      }
      return new Response(row.mask, {
        status: 200,
        headers: { "Content-Type": "image/png" },
      });
    }

    return new Response("Not found", { status: 404 });
  }

  async webSocketMessage(ws, message) {
    if (typeof message === "string") return;
    const { header, payload } = unpackFrame(message);

    if (header.type === "done" && header.job_id) {
      this.ctx.storage.sql.exec(
        "INSERT OR REPLACE INTO results (job_id, ok, error, mask, created_at) VALUES (?, 1, NULL, ?, ?)",
        header.job_id,
        payload,
        Date.now()
      );
    } else if (header.type === "err" && header.job_id) {
      this.ctx.storage.sql.exec(
        "INSERT OR REPLACE INTO results (job_id, ok, error, mask, created_at) VALUES (?, 0, ?, NULL, ?)",
        header.job_id,
        header.error || "Worker error",
        Date.now()
      );
    }
  }

  async webSocketClose(ws, code, reason) {
    try {
      ws.close(code, reason);
    } catch {}
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const key = request.headers.get("X-Relay-Key") || url.searchParams.get("key");
    if (!env.RELAY_SECRET || key !== env.RELAY_SECRET) {
      return new Response("Unauthorized", { status: 401 });
    }

    const id = env.RELAY.idFromName("global");
    const stub = env.RELAY.get(id);
    return stub.fetch(request);
  },
};
