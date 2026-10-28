/**
 * Thin TypeScript facade for graph-memory-platform (M4.4) — the 1:1 mirror
 * of `src/graph_memory/channels/sdk.py` ("SDK-thin client, SPEC §9").
 *
 * Thin on purpose, like the Python twin: every method is a plain HTTP call
 * the CLI could make by hand — no retry policies, no caching, no fallback
 * semantics. The complexity budget stays in one place (the platform).
 * This file is the reference facade for docs/interop; cast it into your
 * build (it uses the global `fetch`, Node 18+ / every modern runtime).
 *
 * ════════════════════════════════════════════════════════════════════
 * PARITY CHECKLIST — re-run after any change to EITHER file. The parity
 * unit is: the method set, each method's route + params + header set +
 * default values, and the error policy. Nothing else may grow here
 * before it grows in `channels/sdk.py`.
 *
 *  1. ingest()   → POST {ingestBase}/v1/signals:batch  body {"signals":[…]}
 *  2. context()  → GET  {base}/v1/context/{user}/{capability}
 *  3. iterItems()→ GET  {base}/v1/context/{user}/{capability}/items
 *                   cursor-walk ?limit=N[&cursor=…] until next_cursor=null
 *  4. prefetch() → POST {base}/v1/context/{user}/{capability}/prefetch
 *                   optional Idempotency-Key; replay responses return the
 *                   stored report with the Idempotency-Replayed header
 *  5. Auth is ONE header on every call: Authorization: Bearer <token>
 *  6. Defaults mirror Python: timeout 2.0 s → timeoutMs 2_000;
 *     page_size 20 → pageSize 20
 *  7. Error policy mirrors raise_for_status(): non-2xx throws
 *     GraphMemoryApiError carrying status + body text — no retry, no
 *     fallback. The platform's structured payload
 *     {"detail": {error, code, details}} is on `.body` for callers
 *     that want it; this facade does not interpret it.
 *
 * Translation rules Python → TypeScript (keep stable):
 *   snake_case methods → camelCase (iter_items → iterItems);
 *   `*_s` time suffix → `*Ms` (timeout_s → timeoutMs);
 *   wire-format keys (next_cursor, Idempotency-Key) stay as served;
 *   path segments are percent-encoded (httpx does the same).
 *
 * NOT wrapped here yet (channel-only REST surfaces — plain `fetch()` them,
 * they land here only when `channels/sdk.py` grows their Python faces):
 *   GET   /v1/search/{user}          (M4.2 hybrid ranked search)
 *   POST  /v1/forget/{user}          (M4.1 governed forget)
 *   GET   /v1/export/{user}          (M4.5 GDPR portability)
 *   POST  /v1/prefetch/{user}        (M4.6 warm-on-login batch)
 * ════════════════════════════════════════════════════════════════════
 *
 * Usage (dev posture — `platform serve` ports: ingest 8001, rest 8002):
 *
 *   const memory = new GraphMemoryClient({
 *     baseUrl: "http://127.0.0.1:8002",
 *     token: "dev-acme-token",           // speaks for tenant `acme`
 *     ingestBaseUrl: "http://127.0.0.1:8001",
 *   });
 *   await memory.ingest([{ id: "…", source: "…", type: "itf.skill_assessed.v1", … }]);
 *   const snapshot = await memory.context("user-1", "skills");
 *   for await (const item of memory.iterItems("user-1", "skills")) { … }
 */

export interface GraphMemoryClientOptions {
  /** REST plane of `platform serve` (default port 8002). */
  baseUrl: string;
  /** Bearer token; the tenant rides the credential, never a parameter. */
  token: string;
  /** Per-call timeout. Default 2_000 ms (Python: timeout_s = 2.0). */
  timeoutMs?: number;
  /** Ingest plane (default port 8001); falls back to baseUrl, as in Python. */
  ingestBaseUrl?: string;
}

/** Mirrors httpx.HTTPStatusError with the platform's payload attached. */
export class GraphMemoryApiError extends Error {
  readonly status: number;
  /** Raw body text — the platform's {detail:{error,code,details}} on errors. */
  readonly body: string;

  constructor(status: number, body: string) {
    super(`graph-memory-platform responded ${status}`);
    this.name = "GraphMemoryApiError";
    this.status = status;
    this.body = body;
  }
}

export type JsonBody = Record<string, unknown>;

export interface IterItemsOptions {
  /** Default 20 (Python: page_size = 20). */
  pageSize?: number;
}

export interface PrefetchOptions {
  /** Idempotency-Key header; replays return the stored report. */
  idempotencyKey?: string;
}

export class GraphMemoryClient {
  readonly #base: string;
  readonly #ingestBase: string;
  readonly #token: string;
  readonly #timeoutMs: number;

  constructor(options: GraphMemoryClientOptions) {
    this.#base = options.baseUrl.replace(/\/+$/, "");
    this.#ingestBase = (options.ingestBaseUrl ?? options.baseUrl).replace(
      /\/+$/,
      "",
    );
    this.#token = options.token;
    this.#timeoutMs = options.timeoutMs ?? 2_000;
  }

  /** POST /v1/signals:batch (the ingest edge). */
  async ingest(signals: readonly JsonBody[]): Promise<JsonBody> {
    const response = await fetch(`${this.#ingestBase}/v1/signals:batch`, {
      method: "POST",
      headers: this.#auth("application/json"),
      body: JSON.stringify({ signals: [...signals] }),
      signal: AbortSignal.timeout(this.#timeoutMs),
    });
    return this.#okJson(response);
  }

  /** GET /v1/context/{user}/{capability} — the full snapshot body. */
  async context(user: string, capability: string): Promise<JsonBody> {
    const response = await fetch(
      `${this.#base}/v1/context/${enc(user)}/${enc(capability)}`,
      { headers: this.#auth(), signal: AbortSignal.timeout(this.#timeoutMs) },
    );
    return this.#okJson(response);
  }

  /**
   * Cursor-walk GET …/items until `next_cursor` is null, yielding each
   * item (Python: iter_items, an Iterator over the same pages).
   */
  async *iterItems(
    user: string,
    capability: string,
    options?: IterItemsOptions,
  ): AsyncGenerator<JsonBody, void, unknown> {
    let cursor: string | null = null;
    while (true) {
      const params = new URLSearchParams({
        limit: String(options?.pageSize ?? 20),
      });
      if (cursor !== null) {
        params.set("cursor", cursor);
      }
      const response = await fetch(
        `${
          this.#base
        }/v1/context/${enc(user)}/${enc(capability)}/items?${params}`,
        { headers: this.#auth(), signal: AbortSignal.timeout(this.#timeoutMs) },
      );
      const body = (await this.#okJson(response)) as {
        items: JsonBody[];
        next_cursor: string | null;
      };
      yield* body.items;
      cursor = body.next_cursor;
      if (cursor === null) {
        return;
      }
    }
  }

  /** POST …/prefetch; an Idempotency-Key replays the stored response. */
  async prefetch(
    user: string,
    capability: string,
    options?: PrefetchOptions,
  ): Promise<JsonBody> {
    const headers = this.#auth();
    if (options?.idempotencyKey !== undefined) {
      headers.set("Idempotency-Key", options.idempotencyKey);
    }
    // No body — the Python twin posts headers only (httpx json-less POST).
    const response = await fetch(
      `${this.#base}/v1/context/${enc(user)}/${enc(capability)}/prefetch`,
      { method: "POST", headers, signal: AbortSignal.timeout(this.#timeoutMs) },
    );
    return this.#okJson(response);
  }

  /**
   * Interface parity with the Python client (which closes its pooled
   * httpx.Client here). The global `fetch` holds no per-client state,
   * so this is a no-op kept so `using`-style cleanup code ports 1:1.
   */
  close(): void {
    // no state to release — parity with sdk.py's close(), kept deliberately
  }

  #auth(contentType?: string): Headers {
    const headers = new Headers({
      Authorization: `Bearer ${this.#token}`,
    });
    if (contentType !== undefined) {
      headers.set("Content-Type", contentType);
    }
    return headers;
  }

  async #okJson(response: Response): Promise<JsonBody> {
    const text = await response.text();
    if (!response.ok) {
      // raise_for_status() equivalent; the payload rides on `.body`.
      throw new GraphMemoryApiError(response.status, text);
    }
    return JSON.parse(text) as JsonBody;
  }
}

/** httpx percent-encodes path specials; mirror it. */
function enc(value: string): string {
  return encodeURIComponent(value);
}
