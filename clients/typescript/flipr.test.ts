// Tests for the TypeScript flipr client.
//
// The case list mirrors clients/go/flipr_test.go one for one -- the Go client
// is canonical, so where behaviour is shared these assert the same thing under
// the same name. The extra blocks at the end cover what this client has and
// the Go one does not: the full Value oneof, the metrics sink, and the bounded
// read.

import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  Client,
  ConfigError,
  RpcError,
  Policy,
  boolValue,
  formatValue,
  intValue,
  stringValue,
  type Config,
  type Flag,
  type Metrics,
  NO_CACHE,
  clientHeader,
} from "./flipr.js";

// --- harness ---------------------------------------------------------------

/** A logger that records instead of printing. */
function recorder() {
  const lines: { level: string; msg: string; fields?: Record<string, unknown> }[] = [];
  return {
    lines,
    log: {
      info: (msg: string, fields?: Record<string, unknown>) => void lines.push({ level: "info", msg, ...(fields ? { fields } : {}) }),
      warn: (msg: string, fields?: Record<string, unknown>) => void lines.push({ level: "warn", msg, ...(fields ? { fields } : {}) }),
      error: (msg: string, fields?: Record<string, unknown>) => void lines.push({ level: "error", msg, ...(fields ? { fields } : {}) }),
    },
  };
}

/** A metrics sink that records every call, for asserting instrumentation. */
function meter(): Metrics & { calls: string[] } {
  const calls: string[] = [];
  return {
    calls,
    inc: (n, l, by) => void calls.push(`inc ${n}${JSON.stringify(l)} ${by}`),
    gauge: (n, l, v) => void calls.push(`gauge ${n}${JSON.stringify(l)} ${v}`),
    observe: (n, l) => void calls.push(`observe ${n}${JSON.stringify(l)}`),
  };
}

/** One canned flipr. Each entry is a method name to a response factory. */
type Handlers = Record<string, () => { status?: number; body: unknown; headers?: Record<string, string> }>;

/** Build a fetch over a handler table, recording requests. */
function fakeFlipr(handlers: Handlers) {
  const requests: { method: string; caller: string | null; client: string | null; body: unknown }[] = [];
  const impl = (async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(input);
    const method = url.split("/").pop() ?? "";
    const headers = new Headers(init?.headers ?? {});
    requests.push({
      method,
      caller: headers.get("X-Flipr-Caller"),
      client: headers.get("X-Flipr-Client"),
      body: init?.body ? JSON.parse(String(init.body)) : undefined,
    });
    let handler = handlers[method];
    // The contract reads the namespace whole; a fake written for GetFlag
    // serves GetNamespace by wrapping its one flag, so the older scenarios
    // keep meaning what they meant.
    if (!handler && method === "GetNamespace" && handlers.GetFlag) {
      const getFlag = handlers.GetFlag;
      handler = () => {
        const r = getFlag() as { status?: number; body: { flag?: { key?: string; value?: unknown } } };
        if ((r.status ?? 200) >= 400) return r;
        const flag = r.body.flag ?? {};
        // the older fakes answer any key with one value; serve it under every
        // key a scenario declares so a typed read finds its own
        const keys = ["smc.enabled", "render.enabled", "model.backend", "batch.size"];
        const flags = flag.key ? [{ key: flag.key, value: flag.value }] : keys.map((key) => ({ key, value: flag.value }));
        return { body: { namespace: { service: "metricsd", version: "v1", flags } } };
      };
    }
    if (!handler) throw new Error(`no handler for ${method}`);
    const { status = 200, body, headers: respHeaders = {} } = handler() as { status?: number; body: unknown; headers?: Record<string, string> };
    const text = typeof body === "string" ? body : JSON.stringify(body);
    return {
      ok: status < 400,
      status,
      headers: new Headers({ "content-type": typeof body === "string" ? "text/plain" : "application/json", ...respHeaders }),
      text: async () => text,
    } as Response;
  }) as typeof fetch;
  return { impl, requests };
}

const DECLARED: Flag[] = [
  { key: "smc.enabled", value: boolValue(true), desc: "on: series published. off: absent within one interval.", expensive: false },
  { key: "render.enabled", value: boolValue(false), desc: "on: a browser runs per job. off: refused before launch.", expensive: true },
];

/** A Config with sane defaults for a test. */
function config(over: Partial<Config> = {}): Config {
  const { log } = recorder();
  return {
    service: "metricsd",
    version: "v1",
    url: "http://flipr.test",
    envPrefix: "METRICSD",
    declared: DECLARED,
    onUnknown: Policy.Refuse,
    log,
    env: {},
    // Tests do not sleep and do not roll dice: the retry loop is exercised for
    // its DECISIONS, and its timing is asserted separately below.
    sleepImpl: async () => {},
    randomImpl: () => 0.5,
    // the tests read fresh so a flip lands on the next check; the cache has
    // its own scenario below
    cacheTtlMs: NO_CACHE,
    ...over,
  };
}

/** The happy-path flipr: alive, one generation, answers GetFlag with `on`. */
function liveFlipr(on = true, generation = "gen-1") {
  return {
    Ping: () => ({ body: { version: "test", storeGeneration: generation } }),
    PublishNamespace: () => ({ body: { flagsPublished: 2 } }),
    GetFlag: () => ({ body: { flag: { key: "smc.enabled", value: { boolValue: on } } } }),
  } satisfies Handlers;
}

// --- mirrored from flipr_test.go -------------------------------------------

describe("unknown splits on policy", () => {
  it("Refuse turns an unreachable flipr into a stop", async () => {
    let alive = true;
    const { impl } = fakeFlipr({
      ...liveFlipr(true),
      Ping: () => (alive ? { body: { storeGeneration: "gen-1" } } : { status: 503, body: {} }),
    });
    const c = await Client.connect(config({ fetchImpl: impl, onUnknown: Policy.Refuse }));
    expect((await c.enabled("smc.enabled")).on).toBe(true);
    alive = false;
    const r = await c.check("smc.enabled");
    expect(r.known).toBe(false);
    expect(r.value).toEqual(boolValue(false));
    expect(r.why).toContain("gates fail STOP");
  });

  it("Hold keeps the last known answer and says it is holding", async () => {
    let alive = true;
    const { impl } = fakeFlipr({
      ...liveFlipr(true),
      Ping: () => (alive ? { body: { storeGeneration: "gen-1" } } : { status: 503, body: {} }),
    });
    const c = await Client.connect(config({ fetchImpl: impl, onUnknown: Policy.Hold }));
    expect((await c.enabled("smc.enabled")).on).toBe(true);
    alive = false;
    const r = await c.check("smc.enabled");
    expect(r.known).toBe(false);
    expect(r.value).toEqual(boolValue(true));
    expect(r.why).toContain("holding last known");
  });
});

it("Hold with no prior read uses the declared default", async () => {
  const { impl } = fakeFlipr({ Ping: () => ({ status: 503, body: {} }) });
  // Boot fails; the client stays in flipr mode and Hold, with nothing ever
  // read, holds the declared default and says it is holding.
  const c = await Client.connect(config({ fetchImpl: impl, onUnknown: Policy.Hold, retry: { attempts: 1, baseMs: 1, capMs: 1 } }));
  expect(c.mode()).toBe("flipr");
  const a = await c.check("smc.enabled");
  expect(a.value).toEqual(boolValue(true));
  expect(a.known).toBe(false);
  expect((await c.check("render.enabled")).value).toEqual(boolValue(false));
});

it("reads the value fresh on every check", async () => {
  let on = true;
  const { impl, requests } = fakeFlipr({
    ...liveFlipr(),
    GetFlag: () => ({ body: { flag: { value: { boolValue: on } } } }),
  });
  const c = await Client.connect(config({ fetchImpl: impl }));
  expect((await c.enabled("smc.enabled")).on).toBe(true);
  on = false;
  // An operator flip lands on the NEXT check; nothing is cached across reads.
  expect((await c.enabled("smc.enabled")).on).toBe(false);
  expect(requests.filter((r) => r.method === "GetNamespace")).toHaveLength(2);
});

it("republishes declared defaults when the store generation changes", async () => {
  let generation = "gen-1";
  const { impl, requests } = fakeFlipr({
    ...liveFlipr(),
    Ping: () => ({ body: { storeGeneration: generation } }),
  });
  const { log, lines } = recorder();
  const c = await Client.connect(config({ fetchImpl: impl, log }));
  expect(requests.filter((r) => r.method === "PublishNamespace")).toHaveLength(1);

  generation = "gen-2"; // the store was wiped and rebuilt
  await c.check("smc.enabled");
  expect(requests.filter((r) => r.method === "PublishNamespace")).toHaveLength(2);
  expect(lines.some((l) => l.level === "warn" && l.msg.includes("generation changed"))).toBe(true);
});

it("publishes its declarations at boot", async () => {
  const { impl, requests } = fakeFlipr(liveFlipr());
  await Client.connect(config({ fetchImpl: impl }));
  const publish = requests.find((r) => r.method === "PublishNamespace");
  const ns = (publish?.body as { namespace: { service: string; version: string; flags: unknown[] } }).namespace;
  expect(ns.service).toBe("metricsd");
  expect(ns.version).toBe("v1");
  expect(ns.flags).toHaveLength(2);
});

it("a flipr that is configured and dead at boot applies the policy, not env mode, and publishes on the first answer", async () => {
  let down = true;
  const { impl, requests } = fakeFlipr({
    ...liveFlipr(),
    Ping: () => (down ? { status: 503, body: {} } : { body: { storeGeneration: "gen-1" } }),
  });
  const { log, lines } = recorder();
  const c = await Client.connect(config({ fetchImpl: impl, log, retry: { attempts: 1, baseMs: 1, capMs: 1 } }));
  expect(c.mode()).toBe("flipr");
  // LOUD, never silent; and Refuse refuses: an outage must not make a service more willing to spend
  expect(lines.some((l) => l.level === "error" && l.msg.includes("unreachable at boot"))).toBe(true);
  const r = await c.check("smc.enabled");
  expect(r.known).toBe(false);
  expect(r.why).toContain("STOP");
  expect(requests.filter((q) => q.method === "PublishNamespace")).toHaveLength(0);
  // flipr answers: the next check is the promotion and the declarations are published on it
  down = false;
  expect((await c.enabled("smc.enabled")).on).toBe(true);
  expect(requests.filter((q) => q.method === "PublishNamespace")).toHaveLength(1);
});

it("a refused publish stays in flipr mode so the refusal stays visible", async () => {
  const { impl } = fakeFlipr({
    ...liveFlipr(),
    PublishNamespace: () => ({ status: 400, body: { error: "namespace budget exceeded" } }),
  });
  const { log, lines } = recorder();
  const c = await Client.connect(config({ fetchImpl: impl, log }));
  expect(c.mode()).toBe("flipr");
  expect(lines.some((l) => l.level === "error" && l.msg.includes("publish refused"))).toBe(true);
});

describe("env mode reads the prefixed variable", () => {
  it("maps smc.enabled to METRICSD_SMC_ENABLED, not _ENABLED_ENABLED", async () => {
    const c = await Client.connect(config({ url: "", env: { METRICSD_SMC_ENABLED: "0" } }));
    const r = await c.check("smc.enabled");
    expect(r.value).toEqual(boolValue(false));
    expect(r.known).toBe(true);
    expect(r.why).toContain("METRICSD_SMC_ENABLED");
  });

  it("accepts 1/true/0/false case-insensitively", async () => {
    for (const [raw, want] of [["1", true], ["TRUE", true], ["0", false], ["False", false]] as const) {
      const c = await Client.connect(config({ url: "", env: { METRICSD_SMC_ENABLED: raw } }));
      expect((await c.check("smc.enabled")).value).toEqual(boolValue(want));
    }
  });

  it("falls back to the declaration on a non-boolean, and says so, still known", async () => {
    const c = await Client.connect(config({ url: "", env: { METRICSD_SMC_ENABLED: "yes please" } }));
    const r = await c.check("smc.enabled");
    expect(r.value).toEqual(boolValue(true)); // the declared default
    expect(r.known).toBe(true); // the operator wrote it; nothing went unanswered
    expect(r.why).toContain("is not a boolean");
  });

  it("every env answer is known, including the refusals", async () => {
    const c = await Client.connect(config({ url: "", env: {} }));
    expect((await c.check("render.enabled")).known).toBe(true);
  });
});

it("an undeclared key is never on by default", async () => {
  const c = await Client.connect(config({ url: "" }));
  const r = await c.check("nobody.declared.this");
  expect(r.value).toBeUndefined();
  expect((await c.enabled("nobody.declared.this")).on).toBe(false);
});

describe("config validation", () => {
  const cases: [string, Partial<Config>][] = [
    ["service", { service: "" }],
    ["version", { version: "" }],
    ["envPrefix", { envPrefix: "" }],
  ];
  for (const [name, over] of cases) {
    it(`rejects a missing ${name}`, async () => {
      await expect(Client.connect(config(over))).rejects.toThrow(ConfigError);
    });
  }

  it("rejects a missing policy, because there is no safe default", async () => {
    await expect(
      Client.connect(config({ onUnknown: undefined as unknown as Policy })),
    ).rejects.toThrow(/no safe default/);
  });

  it("does NOT reject a flipr that is merely down", async () => {
    const { impl } = fakeFlipr({ Ping: () => ({ status: 503, body: {} }) });
    await expect(Client.connect(config({ fetchImpl: impl }))).resolves.toBeInstanceOf(Client);
  });
});

it("declares its caller on every call", async () => {
  const { impl, requests } = fakeFlipr(liveFlipr());
  const c = await Client.connect(config({ fetchImpl: impl }));
  await c.check("smc.enabled");
  expect(requests.length).toBeGreaterThan(0);
  for (const r of requests) expect(r.caller).toBe("metricsd@v1");
});

// --- beyond the Go client --------------------------------------------------

describe("the whole Value oneof", () => {
  const typed: Flag[] = [
    { key: "model.backend", value: stringValue("ollama"), desc: "which backend serves model calls", expensive: false },
    { key: "batch.size", value: intValue(64n), desc: "rows per batch", expensive: false },
  ];

  it("reads a string flag", async () => {
    const { impl } = fakeFlipr({
      ...liveFlipr(),
      GetFlag: () => ({ body: { flag: { value: { stringValue: "claude" } } } }),
    });
    const c = await Client.connect(config({ fetchImpl: impl, declared: typed }));
    expect((await c.checkString("model.backend")).value).toBe("claude");
  });

  it("reads an int flag as a bigint, without losing precision", async () => {
    const big = "9007199254740993"; // 2^53 + 1: a number would round this
    const { impl } = fakeFlipr({
      ...liveFlipr(),
      GetFlag: () => ({ body: { flag: { value: { intValue: big } } } }),
    });
    const c = await Client.connect(config({ fetchImpl: impl, declared: typed }));
    expect((await c.checkInt("batch.size")).value).toBe(BigInt(big));
  });

  it("refuses to treat a string flag as a gate", async () => {
    const { impl } = fakeFlipr({
      ...liveFlipr(),
      GetFlag: () => ({ body: { flag: { value: { stringValue: "false" } } } }),
    });
    const c = await Client.connect(config({ fetchImpl: impl, declared: typed }));
    // The string "false" must not become a boolean answer in either direction.
    const { on, why } = await c.enabled("model.backend");
    expect(on).toBe(false);
    expect(why).toContain("is a string flag, not a gate");
  });

  it("reads a string flag literally from the environment", async () => {
    const c = await Client.connect(
      config({ url: "", declared: typed, env: { METRICSD_MODEL_BACKEND: "claude" } }),
    );
    expect((await c.checkString("model.backend")).value).toBe("claude");
  });

  it("distinguishes an absent value from false", async () => {
    const { impl } = fakeFlipr({ ...liveFlipr(), GetFlag: () => ({ body: { flag: { key: "smc.enabled" } } }) });
    const c = await Client.connect(config({ fetchImpl: impl }));
    const r = await c.check("smc.enabled");
    expect(r.value).toBeUndefined();
    expect(r.known).toBe(true);
    expect(r.why).toContain("has no value");
  });

  it("formats values for a log line", () => {
    expect(formatValue(boolValue(false))).toBe("false");
    expect(formatValue(stringValue("ollama"))).toBe('"ollama"');
    expect(formatValue(intValue(64n))).toBe("64");
    expect(formatValue(undefined)).toBe("(absent)");
  });
});

describe("instrumentation", () => {
  it("declares a series per flag at zero before anything happens", async () => {
    const m = meter();
    const { impl } = fakeFlipr(liveFlipr());
    await Client.connect(config({ fetchImpl: impl, metrics: m }));
    expect(m.calls).toContain('inc flipr_client_reads_total{"key":"smc.enabled","outcome":"ok"} 0');
    expect(m.calls).toContain('inc flipr_client_reads_total{"key":"smc.enabled","outcome":"unknown"} 0');
  });

  it("publishes flag state as a gauge so a flip is visible on a graph", async () => {
    const m = meter();
    const { impl } = fakeFlipr({ ...liveFlipr(false) });
    const c = await Client.connect(config({ fetchImpl: impl, metrics: m }));
    await c.check("smc.enabled");
    expect(m.calls).toContain('gauge flipr_client_flag_state{"key":"smc.enabled"} 0');
  });

  it("reports up=0 in env mode", async () => {
    const m = meter();
    await Client.connect(config({ url: "", metrics: m }));
    expect(m.calls).toContain("gauge flipr_client_up{} 0");
  });

  it("counts rpcs and times them", async () => {
    const m = meter();
    const { impl } = fakeFlipr(liveFlipr());
    await Client.connect(config({ fetchImpl: impl, metrics: m }));
    expect(m.calls).toContain('inc flipr_client_requests_total{"method":"Ping","outcome":"value"} 1');
    expect(m.calls).toContain('observe flipr_client_duration_seconds{"method":"Ping"}');
  });

  it("counts a store wipe", async () => {
    const m = meter();
    let generation = "gen-1";
    const { impl } = fakeFlipr({ ...liveFlipr(), Ping: () => ({ body: { storeGeneration: generation } }) });
    const c = await Client.connect(config({ fetchImpl: impl, metrics: m }));
    generation = "gen-2";
    await c.check("smc.enabled");
    expect(m.calls).toContain("inc flipr_client_generation_changes_total{} 1");
  });

  it("costs nothing when no sink is given", async () => {
    const { impl } = fakeFlipr(liveFlipr());
    await expect(Client.connect(config({ fetchImpl: impl }))).resolves.toBeInstanceOf(Client);
  });
});

describe("transport", () => {
  it("refuses a response past the bound rather than allocating it", async () => {
    const impl = (async () => ({
      ok: true,
      status: 200,
      text: async () => "x".repeat((1 << 22) + 1),
    })) as unknown as typeof fetch;
    const { log, lines } = recorder();
    const c = await Client.connect(config({ fetchImpl: impl, log }));
    // Boot could not parse a ping: the bound held, and the outage is loud
    expect(c.mode()).toBe("flipr");
    expect(lines.some((l) => l.level === "error" && l.msg.includes("unreachable at boot"))).toBe(true);
  });

  it("aborts a request that outlives the timeout", async () => {
    const impl = ((_i: RequestInfo | URL, init?: RequestInit) =>
      new Promise((_resolve, reject) => {
        init?.signal?.addEventListener("abort", () => reject(new Error("aborted")));
      })) as unknown as typeof fetch;
    const { log, lines } = recorder();
    const c = await Client.connect(config({ fetchImpl: impl, timeoutMs: 10, log, retry: { attempts: 1, baseMs: 1, capMs: 1 } }));
    expect(c.mode()).toBe("flipr");
    expect(lines.some((l) => l.msg.includes("unreachable at boot"))).toBe(true);
  });

  it("exposes its namespace and declarations for a health handler", async () => {
    const { impl } = fakeFlipr(liveFlipr());
    const c = await Client.connect(config({ fetchImpl: impl }));
    expect(c.namespace()).toBe("metricsd@v1");
    expect(c.declared()).toHaveLength(2);
  });

  it("fetches the whole namespace in one round trip", async () => {
    const { impl } = fakeFlipr({
      ...liveFlipr(),
      GetNamespace: () => ({
        body: { namespace: { service: "metricsd", version: "v1", flags: [{ key: "smc.enabled", value: { boolValue: true } }] } },
      }),
    });
    const c = await Client.connect(config({ fetchImpl: impl }));
    const ns = await c.snapshot();
    expect(ns?.flags).toHaveLength(1);
  });

  it("has no namespace to snapshot in env mode", async () => {
    const c = await Client.connect(config({ url: "" }));
    expect(await c.snapshot()).toBeUndefined();
  });
});


describe("bounded retry with full jitter", () => {
  /**
   * A fetch whose PING fails `failures` times and then succeeds.
   *
   * Scoped to Ping deliberately: connect() also publishes, and a harness that
   * answered every method with one canned body would have publish failing on
   * an unknown-field decode and retrying too, which counts the wrong thing.
   */
  function flaky(failures: number, status?: number) {
    let pings = 0;
    const impl = (async (input: RequestInfo | URL) => {
      const method = String(input).split("/").pop();
      if (method !== "Ping") {
        return { ok: true, status: 200, headers: new Headers(), text: async () => "{}" } as Response;
      }
      pings++;
      if (pings <= failures) {
        if (status === undefined) throw new Error("ECONNREFUSED");
        return { ok: false, status, headers: new Headers(), text: async () => "nope" } as Response;
      }
      return { ok: true, status: 200, headers: new Headers(), text: async () => JSON.stringify({ storeGeneration: "gen-1" }),
      } as Response;
    }) as typeof fetch;
    return { impl, calls: () => pings };
  }

  it("retries a refused connection and succeeds on a later attempt", async () => {
    const { impl, calls } = flaky(2);
    const c = await Client.connect(config({ fetchImpl: impl, declared: DECLARED.slice(0, 1) }));
    // Boot pinged, failed twice, succeeded on the third: flipr mode, not env.
    expect(c.mode()).toBe("flipr");
    expect(calls()).toBe(3);
  });

  it("retries a 5xx -- that is what a restart looks like", async () => {
    const { impl, calls } = flaky(1, 503);
    const c = await Client.connect(config({ fetchImpl: impl, declared: DECLARED.slice(0, 1) }));
    expect(c.mode()).toBe("flipr");
    expect(calls()).toBe(2);
  });

  it("retries a 429", async () => {
    const { impl, calls } = flaky(1, 429);
    await Client.connect(config({ fetchImpl: impl, declared: DECLARED.slice(0, 1) }));
    expect(calls()).toBe(2);
  });

  it("does NOT retry a 4xx -- a contract failure is not a transient", async () => {
    const { impl, calls } = flaky(99, 400);
    const c = await Client.connect(config({ fetchImpl: impl, declared: DECLARED.slice(0, 1) }));
    expect(c.mode()).toBe("flipr"); // boot gave up, and the policy applies
    expect(calls()).toBe(1); // and did not ask the same wrong question again
  });

  it("gives up after the configured attempts rather than stalling the caller", async () => {
    const { impl, calls } = flaky(99);
    const c = await Client.connect(
      config({ fetchImpl: impl, declared: DECLARED.slice(0, 1), retry: { attempts: 4, baseMs: 1, capMs: 10 } }),
    );
    expect(c.mode()).toBe("flipr");
    expect(calls()).toBe(4);
  });

  it("attempts:1 disables retry entirely", async () => {
    const { impl, calls } = flaky(99);
    await Client.connect(
      config({ fetchImpl: impl, declared: DECLARED.slice(0, 1), retry: { attempts: 1, baseMs: 1, capMs: 10 } }),
    );
    expect(calls()).toBe(1);
  });

  it("sleeps a JITTERED, doubling, capped window", async () => {
    const slept: number[] = [];
    const { impl } = flaky(99);
    await Client.connect(
      config({
        fetchImpl: impl,
        declared: DECLARED.slice(0, 1),
        retry: { attempts: 5, baseMs: 100, capMs: 300 },
        sleepImpl: async (ms) => void slept.push(ms),
        randomImpl: () => 1, // the TOP of each window, to show the shape
      }),
    );
    // Windows double -- 100, 200 -- then hit the 300 cap and stay there.
    expect(slept).toEqual([100, 200, 300, 300]);
  });

  it("draws from [0, window), so a recovering herd spreads out", async () => {
    const slept: number[] = [];
    const { impl } = flaky(99);
    await Client.connect(
      config({
        fetchImpl: impl,
        declared: DECLARED.slice(0, 1),
        retry: { attempts: 3, baseMs: 100, capMs: 1000 },
        sleepImpl: async (ms) => void slept.push(ms),
        randomImpl: () => 0, // full jitter can sleep zero; equal jitter cannot
      }),
    );
    expect(slept).toEqual([0, 0]);
  });

  it("counts retries so a flapping flipr is visible", async () => {
    const m = meter();
    const { impl } = flaky(2);
    await Client.connect(config({ fetchImpl: impl, declared: DECLARED.slice(0, 1), metrics: m }));
    expect(m.calls.filter((c) => c.startsWith("inc flipr_client_retries_total"))).toHaveLength(2);
  });

  it("reports one rpc outcome per call, not per attempt", async () => {
    const m = meter();
    const { impl } = flaky(2);
    await Client.connect(config({ fetchImpl: impl, declared: DECLARED.slice(0, 1), metrics: m }));
    expect(m.calls.filter((c) => c.includes('requests_total{"method":"Ping","outcome":"value"}'))).toHaveLength(1);
  });

  it("classifies retryability off the status", () => {
    expect(new RpcError("Ping", "boom").status).toBeUndefined();
    expect(new RpcError("Ping", "boom", 503).status).toBe(503);
  });
});

// --- the contract (clients/CONTRACT.md) -------------------------------------

describe("the contract", () => {
  it("names the caller and the client on every request", async () => {
    const { impl, requests } = fakeFlipr(liveFlipr());
    await Client.connect(config({ fetchImpl: impl }));
    for (const r of requests) {
      expect(r.caller).toBe("metricsd@v1");
      expect(r.client).toBe(clientHeader());
      expect(r.client).toMatch(/^ts\//);
    }
  });

  it("serves a check inside the cache TTL from the last refresh and refreshes after it", async () => {
    let on = true;
    const { impl, requests } = fakeFlipr({
      ...liveFlipr(),
      GetFlag: () => ({ body: { flag: { key: "smc.enabled", value: { boolValue: on } } } }),
    });
    const c = await Client.connect(config({ fetchImpl: impl, cacheTtlMs: 1000 }));
    // first read refreshes
    expect((await c.enabled("smc.enabled")).on).toBe(true);
    const trips = requests.filter((r) => r.method === "GetNamespace").length;
    on = false;
    // inside the TTL: a ping (always check flipr is there), no
    // namespace trip, the cached value
    const pings = requests.filter((r) => r.method === "Ping").length;
    expect((await c.enabled("smc.enabled")).on).toBe(true);
    expect(requests.filter((r) => r.method === "GetNamespace").length).toBe(trips);
    expect(requests.filter((r) => r.method === "Ping").length).toBe(pings + 1);
    // after the TTL: the flip lands
    await new Promise((resolve) => setTimeout(resolve, 1050));
    expect((await c.enabled("smc.enabled")).on).toBe(false);
    expect(requests.filter((r) => r.method === "GetNamespace").length).toBe(trips + 1);
  });

  it("treats a lock as its own outcome, remembers it, and clears on the next good answer", async () => {
    let locked = true;
    let requests = 0;
    const slept: number[] = [];
    const m = meter();
    const { impl } = fakeFlipr({
      Ping: () => {
        requests++;
        return locked
          ? { status: 423, body: { error: "locked: restore to 4127" }, headers: { "retry-after": "2" } }
          : { body: { storeGeneration: "gen-1" } };
      },
      PublishNamespace: () => ({ body: {} }),
      GetFlag: () => ({ body: { flag: { key: "smc.enabled", value: { boolValue: true } } } }),
    });
    // connect meets the lock with exactly one request (a lock is not retried
    // inside the call) and stays in flipr mode with the lock remembered
    const c = await Client.connect(
      config({ fetchImpl: impl, onUnknown: Policy.Hold, metrics: m, retry: { attempts: 3, baseMs: 50, capMs: 1000 }, sleepImpl: async (ms) => void slept.push(ms) }),
    );
    expect(c.mode()).toBe("flipr");
    expect(requests).toBe(1);
    expect(slept).toEqual([]);
    expect(m.calls).toContain('inc flipr_client_requests_total{"method":"Ping","outcome":"locked"} 1');
    expect(c.locked()).toEqual({ locked: true, why: "locked: restore to 4127" });
    // a fresh client after unlock is known and carries no lock
    locked = false;
    const d = await Client.connect(config({ fetchImpl: impl, onUnknown: Policy.Hold }));
    expect((await d.enabled("smc.enabled")).on).toBe(true);
    expect(d.locked().locked).toBe(false);
  });

  it("answers from a remembered lock with no request until its deadline", async () => {
    let requests = 0;
    const { impl } = fakeFlipr({
      Ping: () => {
        requests++;
        return { status: 423, body: { error: "locked: restore" }, headers: { "retry-after": "1" } };
      },
      PublishNamespace: () => ({ body: {} }),
      GetFlag: () => ({ body: { flag: { key: "smc.enabled", value: { boolValue: true } } } }),
    });
    const c = await Client.connect(config({ fetchImpl: impl, onUnknown: Policy.Hold, url: "http://flipr.test" }));
    // connect made one request, met the lock, and stays in flipr mode
    expect(requests).toBe(1);
    const r = await c.check("smc.enabled");
    expect(r.known).toBe(false);
    expect(r.why).toContain("423");
    expect(requests).toBe(1); // inside the deadline: no request
    await new Promise((resolve) => setTimeout(resolve, 1050));
    await c.check("smc.enabled");
    expect(requests).toBe(2); // after it: exactly one, and a new deadline
    await c.check("smc.enabled");
    expect(requests).toBe(2);
  });

  it("treats the edge's 404 as unreachable and flipr's own 404 as missing", async () => {
    const edge = meter();
    const { impl: edgeImpl } = fakeFlipr({ Ping: () => ({ status: 404, body: "404 page not found" }) });
    const c = await Client.connect(config({ fetchImpl: edgeImpl, metrics: edge, retry: { attempts: 2, baseMs: 1, capMs: 1 } }));
    expect(c.mode()).toBe("flipr");
    expect(edge.calls).toContain('inc flipr_client_retries_total{"method":"Ping"} 1');
    expect(edge.calls).toContain('inc flipr_client_requests_total{"method":"Ping","outcome":"unreachable"} 1');

    const own = meter();
    const { impl: ownImpl } = fakeFlipr({ Ping: () => ({ status: 404, body: { error: "no such namespace" } }) });
    await Client.connect(config({ fetchImpl: ownImpl, metrics: own, retry: { attempts: 3, baseMs: 1, capMs: 1 } }));
    expect(own.calls.filter((x) => x.startsWith("inc flipr_client_retries_total"))).toHaveLength(0);
    expect(own.calls).toContain('inc flipr_client_requests_total{"method":"Ping","outcome":"missing"} 1');
  });

  it("lists every namespace, a tool's verb", async () => {
    const { impl } = fakeFlipr({
      ...liveFlipr(),
      ListNamespaces: () => ({ body: { namespaces: [{ service: "a", version: "v1" }, { service: "b", version: "v1" }] } }),
    });
    const c = await Client.connect(config({ fetchImpl: impl }));
    expect((await c.list()).map((n) => n.service)).toEqual(["a", "b"]);
  });

  it("refuses a commit as the version, an empty declared list, and an address as the url", async () => {
    const { impl } = fakeFlipr(liveFlipr());
    await expect(Client.connect(config({ fetchImpl: impl, version: "29c44ff" }))).rejects.toThrow(ConfigError);
    await expect(Client.connect(config({ fetchImpl: impl, declared: [] }))).rejects.toThrow(ConfigError);
    await expect(Client.connect(config({ fetchImpl: impl, url: "http://10.0.0.5:9800" }))).rejects.toThrow(ConfigError);
    // a name with a port is fine: the throwaway edge is flipr.test:7800
    await expect(Client.connect(config({ fetchImpl: impl, url: "http://flipr.test:7800" }))).resolves.toBeInstanceOf(Client);
  });
});
