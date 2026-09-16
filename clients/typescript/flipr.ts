// The TypeScript client for flipr, the flag store the mesh reads before it
// does anything expensive.
//
// ============================================================================
// THE DOCTRINE
// ============================================================================
//
// flipr's rule is that the cache is a PERFORMANCE OPTIMISATION AND NEVER A
// FALLBACK: a stale flag served quietly is worse than an outage, because an
// outage is something you can see. So every read pings first, and an
// unanswered ping is reported as UNKNOWN rather than papered over.
//
// If flipr is down, the network is down. That is deliberate, and this client
// does not soften it.
//
// ============================================================================
// THE ONE THING YOU MUST CHOOSE
// ============================================================================
//
// What to DO about unknown is the one thing this package refuses to decide,
// because the two right answers point in opposite directions:
//
//   Policy.Refuse   for GATES OVER SPEND. A stale yes authorizes money nobody
//                   approved. Unknown means STOP.
//
//   Policy.Hold     for INSTRUMENTS. Carry on with the last known answer and
//                   publish the uncertainty. Silencing telemetry because the
//                   flag store wobbled removes the instrument in exactly the
//                   incident it exists for; a hole in a series is not a safe
//                   direction, it is a blind spot.
//
// Picking wrong is quiet in both directions, so there is no default. A Config
// without onUnknown is rejected at construction.
//
// ============================================================================
// TWO MODES, chosen at startup and never mixed
// ============================================================================
//
//   FLIPR MODE   First contact succeeded. Values are read fresh on every
//                check, so an operator flip lands on the next one.
//
//   ENV MODE     No URL, or first contact failed at boot -- logged LOUD, never
//                silent. Gates read <PREFIX>_<KEY>_ENABLED. This is
//                PRE-ONBOARDING behaviour, not a fallback from a flipr that was
//                ever reachable. Env answers are always `known`: nothing went
//                unanswered, there was simply nothing to ask.
//
// A flipr that is DOWN does not fail construction -- a service that will not
// start because a flag store is down is useless in the incident it exists for.
// MISCONFIGURATION does fail, because an operator can flip their way out of one
// and not the other.
//
// ============================================================================
// DEVIATIONS, and why
// ============================================================================
//
// The Go client is canonical and this one mirrors it: same Config fields, same
// Check/Enabled/Namespace/Mode/Declared surface, same two modes, same
// generation self-heal, same env-var derivation, same refusal to call SetFlag.
// Where this client differs, it is for one of the reasons below and no other.
//
//  1. IT SUPPORTS THE WHOLE Value ONEOF. The proto says a flag is bool OR
//     string OR int64, and says why: "nightly data smoothing" set to `surreal`
//     or `claude` would otherwise be two booleans that can both be true. The
//     Go client reads GetBoolValue() and can express only the boolean third of
//     the contract. String flags are already live in the store today
//     (albatross publishes model.backend = "ollama"), so this is a real gap,
//     not a hypothetical one. Modelling the proto rather than the Go client is
//     the deliberate choice here; `enabled()` remains the boolean shorthand
//     that covers the common gate case, so nothing gets harder.
//
//  2. CONSTRUCTION IS ASYNC. Go's New() makes first contact synchronously. A
//     TypeScript constructor cannot await, and a client that returned before
//     it knew its own mode would report `env` for a moment and then change its
//     mind. So the entry point is `await Client.connect(cfg)`. Same
//     semantics, different keyword.
//
//  3. IT IS INSTRUMENTED. The Go client publishes no metrics and exposes
//     Declared() so a caller can build its own series. That leaves the package
//     that decides whether the expensive departments run as the one place with
//     no numbers -- you cannot see on a graph when a flag guarding hours of GPU
//     went on. This client emits a defined set of series through an INJECTED
//     sink (see Metrics), defaulting to a no-op so a consumer that does not
//     want them pays nothing. Declared() is kept anyway, for callers that want
//     to pre-declare their own.
//
//  4. IT RETRIES, with bounded exponential backoff and FULL JITTER. The Go
//     client makes one attempt and reports the failure. That is safe but it is
//     not kind to flipr: every service in the mesh pings on every check, so an
//     outage SYNCHRONISES the whole fleet, and without jitter they all return
//     as one wave against the process that just started -- at the moment it is
//     least able to absorb one. Bounded because the doctrine still says an
//     unanswered read is unknown and the caller is waiting on it. Worth porting
//     back to Go; see the RetryPolicy comment for what is retried and why a 4xx
//     is not.
//
//  5. IT IMPLEMENTS GetNamespace. The proto describes it as "the call a client
//     makes on startup: fetch the whole namespace in one round trip and cache
//     it", so it is squarely a client call; the Go client simply does not have
//     it yet. `snapshot()` is that call.
//
// AND ONE THING IT DELIBERATELY DOES NOT DO. There is no setFlag() and no
// deleteNamespace(). Those are OPERATOR operations: the proto requires a
// `reason` on both precisely because an unexplained flip is how an outage
// becomes unexplainable, and a reason invented by the service that benefits
// from the flip is not a reason. It also keeps the X-Flipr-Caller attribution
// honest -- whoever mines the oplog for who touched a namespace can rule this
// client out at a glance, and that is only true while it remains true.
//
// FINALLY: THIS CLIENT HAS NO FLAGS OF ITS OWN. It never asks flipr whether to
// be a client. Every knob it has is constructor configuration, resolved once,
// because a flag store client whose behaviour depends on the flag store cannot
// be reasoned about when the flag store is the thing that is broken.

import { create, fromJson, toJson, type Message } from "@bufbuild/protobuf";
import { STAMP } from "./stamp.js";
import type { GenMessage } from "@bufbuild/protobuf/codegenv2";
import {
  FlagSchema,
  GetNamespaceRequestSchema,
  GetNamespaceResponseSchema,
  ListNamespacesRequestSchema,
  ListNamespacesResponseSchema,
  NamespaceSchema,
  PingRequestSchema,
  PingResponseSchema,
  PublishNamespaceRequestSchema,
  PublishNamespaceResponseSchema,
  ValueSchema,
  type Namespace,
  type Value,
} from "./flipr/v1/flipr_pb.js";

// ---------------------------------------------------------------------------
// Values
// ---------------------------------------------------------------------------

/**
 * One flag's value, as the proto's `Value` oneof describes it.
 *
 * A discriminated union rather than three optional fields, for the same reason
 * the proto uses a oneof rather than three fields: exactly one case is true at
 * a time, and a shape that can represent two is a shape someone will
 * eventually populate with two.
 *
 * `int` is a bigint because the proto says int64 and a JavaScript number
 * silently loses precision above 2^53. A flag holding a byte budget or an epoch
 * is exactly the kind of value that gets large enough to matter.
 */
export type FlagValue =
  | { readonly kind: "bool"; readonly value: boolean }
  | { readonly kind: "string"; readonly value: string }
  | { readonly kind: "int"; readonly value: bigint };

/** Shorthand for the common case. */
export function boolValue(value: boolean): FlagValue {
  return { kind: "bool", value };
}

/** Shorthand for a string flag, e.g. model.backend = "ollama". */
export function stringValue(value: string): FlagValue {
  return { kind: "string", value };
}

/** Shorthand for an integer flag. Takes a bigint; the wire is int64. */
export function intValue(value: bigint): FlagValue {
  return { kind: "int", value };
}

/** Render a value for a log line or a health document. */
export function formatValue(v: FlagValue | undefined): string {
  if (v === undefined) return "(absent)";
  return v.kind === "string" ? JSON.stringify(v.value) : String(v.value);
}

/** Convert our union into the generated oneof message. */
function toWire(v: FlagValue): Value {
  switch (v.kind) {
    case "bool":
      return create(ValueSchema, { kind: { case: "boolValue", value: v.value } });
    case "string":
      return create(ValueSchema, { kind: { case: "stringValue", value: v.value } });
    case "int":
      return create(ValueSchema, { kind: { case: "intValue", value: v.value } });
  }
}

/**
 * Convert the generated oneof back into our union.
 *
 * Returns undefined for an unset oneof, which is a real case: a flag whose
 * value was never written comes back with no case set, and that is meaningfully
 * different from `false`.
 */
function fromWire(v: Value | undefined): FlagValue | undefined {
  switch (v?.kind.case) {
    case "boolValue":
      return { kind: "bool", value: v.kind.value };
    case "stringValue":
      return { kind: "string", value: v.kind.value };
    case "intValue":
      return { kind: "int", value: v.kind.value };
    default:
      return undefined;
  }
}

// ---------------------------------------------------------------------------
// Policy
// ---------------------------------------------------------------------------

/** What a caller does when nothing authoritative answered. */
export enum Policy {
  /**
   * Unknown never authorizes. For gates over spend, where a stale yes costs
   * money. A boolean read returns false.
   */
  Refuse = "refuse",

  /**
   * Unknown keeps the last known answer, or the declared default if nothing has
   * been read yet. For components whose job is to keep reporting, where going
   * quiet IS the failure. `known: false` on the reading is how the caller
   * publishes that it is holding.
   */
  Hold = "hold",
}

// ---------------------------------------------------------------------------
// Declarations
// ---------------------------------------------------------------------------

/**
 * One declared flag: what a service publishes about itself at boot, so an
 * operator finds it in the store without reading the source.
 */
export interface Flag {
  /** Dotted, and the dots are the hierarchy flipr's UI renders. */
  readonly key: string;

  /** The declared default -- what the flag reads before anyone flips it. */
  readonly value: FlagValue;

  /**
   * What ON does, what OFF does, and what happens to work already in flight.
   * Written for whoever meets it at 03:20 deciding whether flipping it will fix
   * the page or cause the next one.
   */
  readonly desc: string;

  /**
   * Does turning this on cost network, disk, cpu, or a model call?
   *
   * THE EMERGENCY PAGE IS THE SET WHERE THIS IS TRUE. Mark it only for real
   * spend: puffin renders it as `$`, and a cheap flag wearing that marker
   * dilutes the one signal an operator has during an incident.
   */
  readonly expensive: boolean;
}

/** A read, with its value and its epistemics kept separate. */
export interface Reading {
  /** The value to act on, already resolved through the policy. */
  readonly value: FlagValue | undefined;

  /**
   * Whether anything authoritative answered.
   *
   * If your service publishes metrics, PUBLISH THIS TOO. "The flag is off" and
   * "we could not ask" must be distinguishable from outside the process.
   */
  readonly known: boolean;

  /** A human sentence for a log line. Empty when there is nothing to explain. */
  readonly why: string;
}

// ---------------------------------------------------------------------------
// Injected collaborators
// ---------------------------------------------------------------------------

/**
 * The logging surface this client needs.
 *
 * Structural on purpose: it matches console, pino, a bare object, and the
 * estate's own JSON loggers without any of them being a dependency. The Go
 * client requires a *slog.Logger for the same reason -- this client's failures
 * are only useful if they are heard.
 */
export interface Logger {
  info(msg: string, fields?: Record<string, unknown>): void;
  warn(msg: string, fields?: Record<string, unknown>): void;
  error(msg: string, fields?: Record<string, unknown>): void;
}

/**
 * Where this client's metrics go.
 *
 * An interface rather than a registry because a library must not own one: the
 * consumer already has a Prometheus surface and this client's series belong in
 * it, not beside it. Every method is optional to implement in spirit -- the
 * no-op default below satisfies it -- so instrumentation costs a consumer
 * nothing until they want it.
 *
 * The series emitted, all prefixed `flipr_client_`:
 *
 *   rpc_total{method,outcome}            counter, outcome ok|error
 *   rpc_duration_seconds{method}         histogram, seconds
 *   reads_total{key,outcome}             counter, outcome ok|unknown|env
 *   flag_state{key}                      gauge, 1/0 for boolean flags only
 *   up                                   gauge, 1 in flipr mode, 0 in env mode
 *   generation_changes_total             counter, store wipes observed
 *   publish_total{outcome}               counter, outcome ok|refused
 */
export interface Metrics {
  /** Add to a counter. */
  inc(name: string, labels: Record<string, string>, by: number): void;
  /** Set a gauge to an absolute reading. */
  gauge(name: string, labels: Record<string, string>, value: number): void;
  /** Record a duration, in seconds. */
  observe(name: string, labels: Record<string, string>, seconds: number): void;
}

/** The default sink. Costs a function call and does nothing with it. */
const NO_METRICS: Metrics = {
  inc: () => {},
  gauge: () => {},
  observe: () => {},
};

// ---------------------------------------------------------------------------
// Retry
// ---------------------------------------------------------------------------

/**
 * Bounded exponential backoff with full jitter.
 *
 * WHY IT IS BOUNDED. The doctrine says an unanswered read is UNKNOWN and the
 * caller decides what that means. A client that retried until it got an answer
 * would convert "flipr is down" -- a fact the caller needs promptly, because
 * Refuse callers are blocked on it -- into an unbounded stall. So the retry
 * budget is small and the failure still arrives quickly.
 *
 * WHY THE JITTER IS NOT OPTIONAL. Every service in the mesh pings flipr on
 * every check. An outage SYNCHRONISES them: they all fail together, and
 * without jitter they all come back together and arrive as one wave against
 * the service that just started. That is the thundering herd, and the moment
 * it lands is the moment flipr is least able to absorb it. Full jitter --
 * sleeping a random duration in [0, window) rather than the window itself --
 * is what spreads that wave out.
 *
 * WHAT IS RETRIED, and what is not. Transport failures, 429, and 5xx are
 * retried: they are the shapes a restart takes. A 4xx is NOT -- a malformed
 * request or a rejected namespace is a contract failure, and retrying a
 * contract failure just asks the same wrong question three times.
 */
export interface RetryPolicy {
  /** Total attempts including the first. 1 disables retry. Default 3. */
  readonly attempts: number;
  /** First backoff window, doubled per attempt. Default 50ms. */
  readonly baseMs: number;
  /** Ceiling on the window, so backoff cannot outrun the caller. Default 1000ms. */
  readonly capMs: number;
}

/** Small, because flipr is nearby and a blocked gate is waiting on this. */
const DEFAULT_RETRY: RetryPolicy = { attempts: 3, baseMs: 50, capMs: 1000 };

/** Sleep, the boring way. */
function realSleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Full jitter: a random point in [0, window), not the window itself.
 *
 * "Equal jitter" and "decorrelated jitter" both leave a floor under the delay,
 * which keeps some of the synchronisation the jitter exists to destroy. Full
 * jitter is the variant that spreads a recovering herd most evenly, and this
 * is a recovering-herd problem specifically.
 */
function backoffMs(attempt: number, policy: RetryPolicy, random: () => number): number {
  const window = Math.min(policy.capMs, policy.baseMs * 2 ** attempt);
  return Math.floor(random() * window);
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/** Everything a Client needs. Mirrors the Go client's Config field for field. */
export interface Config {
  /** The namespace's service half, e.g. "metricsd". Required. */
  readonly service: string;

  /**
   * The namespace's version half. Required, and PIN IT rather than passing a
   * build commit: per-commit namespaces are born carrying the declared
   * defaults, so every deploy silently resurrects flags an operator killed.
   */
  readonly version: string;

  /** flipr's base URL. Empty selects env mode, deliberately. */
  readonly url: string;

  /** The env-mode variable prefix: "METRICSD" makes smc.enabled read METRICSD_SMC_ENABLED. Required. */
  readonly envPrefix: string;

  /** What this service publishes about itself at boot. */
  readonly declared: readonly Flag[];

  /** The caller's policy. No default; see the header on why guessing is quiet in both directions. */
  readonly onUnknown: Policy;

  /** Required. This client's failures are only useful if they are heard. */
  readonly log: Logger;

  /** Optional metrics sink. Defaults to a no-op. */
  readonly metrics?: Metrics;

  /** Injected so a consumer can test without a live flipr. Defaults to global fetch. */
  readonly fetchImpl?: typeof fetch;

  /** Per-ATTEMPT timeout. flipr is meant to be nearby; slow means broken. Default 5000. */
  readonly timeoutMs?: number;

  /**
   * Bounded retry with exponential backoff and full jitter.
   *
   * See the Retry section in the header for why this is bounded rather than
   * persistent, and why the jitter is not optional. Defaults to three
   * attempts, 50ms base, 1s cap.
   */
  readonly retry?: RetryPolicy;

  /** How long a refreshed namespace answers checks without a trip. Default 5000; NO_CACHE refreshes every check. */
  readonly cacheTtlMs?: number;

  /** Injected so tests do not sleep. Defaults to a real timer. */
  readonly sleepImpl?: (ms: number) => Promise<void>;

  /** Injected so the jitter is reproducible in tests. Defaults to Math.random. */
  readonly randomImpl?: () => number;

  /** Reads the environment in env mode. Injected for testability. Defaults to process.env. */
  readonly env?: Record<string, string | undefined>;
}

/**
 * A Config this client will not accept.
 *
 * Distinct from an unreachable flipr, which is NOT an error: an operator can
 * flip their way out of a down flag store and cannot flip their way out of a
 * service publishing under the wrong name.
 */
export class ConfigError extends Error {
  constructor(message: string) {
    super(`flipr: ${message}`);
    this.name = "ConfigError";
  }
}

/** An RPC that did not produce a usable answer. Internal; surfaced as `why`. */
export class RpcError extends Error {
  /** The HTTP status, when there was one. Absent for a transport failure. */
  readonly status: number | undefined;
  /** Retry-After, in milliseconds, when flipr asked for a wait (429, 423). */
  readonly retryAfterMs: number | undefined;
  /** Whether the answer came from flipr itself (a JSON body) or from the edge. */
  readonly fromFlipr: boolean;
  /** flipr's own error text, for a lock's reason. */
  readonly reason: string;

  constructor(
    method: string,
    detail: string,
    status?: number,
    opts?: { retryAfterMs?: number; fromFlipr?: boolean; reason?: string },
  ) {
    super(`${method}: ${detail}`);
    this.name = "RpcError";
    this.status = status;
    this.retryAfterMs = opts?.retryAfterMs;
    this.fromFlipr = opts?.fromFlipr ?? true;
    this.reason = opts?.reason ?? detail;
  }
}

/** The contract's outcome for a failed call (CONTRACT.md section 3). */
function outcomeOf(err: unknown): "missing" | "unreachable" | "refused" | "locked" {
  if (!(err instanceof RpcError) || err.status === undefined) return "unreachable";
  if (err.status === 423) return "locked";
  if (err.status === 404) return err.fromFlipr ? "missing" : "unreachable";
  if (err.status >= 500 || err.status === 429) return "unreachable";
  return "refused";
}

/**
 * Is this failure the kind a restart looks like?
 *
 * A transport failure has no status: the connection was refused, reset, or
 * timed out, which is exactly what a bouncing flipr looks like from outside.
 * 429 and 5xx are the server saying "not now". Everything else in the 4xx
 * range is the client being wrong, and being wrong more times does not help.
 */
function isRetryable(err: unknown): boolean {
  if (!(err instanceof RpcError)) return true;
  if (err.status === undefined) return true;
  if (err.status === 404) return !err.fromFlipr; // the edge answering for an absent flipr
  // a 423 is not retried inside the call: the lock is the answer, and the
  // client remembers it (CONTRACT.md section 9)
  return err.status === 429 || err.status >= 500;
}

/** A lock is remembered for its Retry-After, capped at this; LOCK_DEFAULT_MS when flipr sent none. */
export const LOCK_CEILING_MS = 30_000;
export const LOCK_DEFAULT_MS = 5_000;

/** Contract default: a flip lands within five seconds. */
export const DEFAULT_CACHE_TTL_MS = 5_000;
/** As cacheTtlMs: every check refreshes. */
export const NO_CACHE = -1;

/**
 * This client's identity on the wire: X-Flipr-Client: ts/<tag>/<hash>
 * (CONTRACT.md sections 1 and 8). Set at package build from the release tag
 * and a sha256 over this file; a build that did not set them announces
 * itself as dev/unknown, which flipr counts as an unknown client.
 */
export const CLIENT_TAG: string = STAMP.tag;
export const CLIENT_HASH: string = STAMP.hash;
export function clientHeader(): string {
  return `ts/${CLIENT_TAG}/${CLIENT_HASH}`;
}

/** An IP host other than loopback: flipr is reached by name (a port on a name is fine). */
function isAddress(url: string): boolean {
  if (url === "") return false;
  let host = url.replace(/^[a-z]+:\/\//i, "").replace(/[/?#].*$/, "");
  if (!host.includes("]")) host = host.replace(/:\d+$/, "");
  if (host === "127.0.0.1" || host === "localhost" || host === "::1" || host === "[::1]") return false;
  return /^\d{1,3}(\.\d{1,3}){3}$/.test(host);
}

// ---------------------------------------------------------------------------
// The client
// ---------------------------------------------------------------------------

/** Reads flags for one service namespace. */
export class Client {
  private readonly cfg: Config;
  private readonly fetchImpl: typeof fetch;
  private readonly metrics: Metrics;
  private readonly env: Record<string, string | undefined>;
  private readonly timeoutMs: number;
  private readonly retry: RetryPolicy;
  private readonly sleep: (ms: number) => Promise<void>;
  private readonly random: () => number;

  /** Empty once first contact has failed: the env-mode switch, as in Go. */
  private base: string;

  /** The store generation last seen, for wipe detection. */
  private generation = "";
  /** The declarations reached flipr at least once. */
  private published = false;

  /** Last value seen per key, for Policy.Hold. Never a fallback in Refuse. */
  private readonly last = new Map<string, FlagValue>();

  /** The last refresh, whole, and when; served inside cacheTtlMs. */
  private cached: Namespace | undefined;
  private refreshedAt = -Infinity;
  private readonly cacheTtlMs: number;

  /** The lock flipr last answered with, if any, for the caller's health line. */
  private lockedWhy: string | undefined;
  /** While it stands, every call answers from the remembered lock with no request. */
  private lockUntil = -Infinity;

  private constructor(cfg: Config, base: string) {
    this.cfg = cfg;
    this.base = base;
    this.fetchImpl = cfg.fetchImpl ?? fetch;
    this.metrics = cfg.metrics ?? NO_METRICS;
    this.env = cfg.env ?? process.env;
    this.timeoutMs = cfg.timeoutMs ?? 5000;
    this.retry = cfg.retry ?? DEFAULT_RETRY;
    this.sleep = cfg.sleepImpl ?? realSleep;
    this.random = cfg.randomImpl ?? Math.random;
    this.cacheTtlMs = cfg.cacheTtlMs ?? DEFAULT_CACHE_TTL_MS;
  }

  /**
   * Validate the config, make first contact, and publish the declarations.
   *
   * Never rejects because flipr is down -- that lands in env mode with a loud
   * log line. DOES reject a misconfigured Config, because a service publishing
   * under the wrong name or with no unknown-policy is a bug an operator cannot
   * flip their way out of.
   */
  static async connect(cfg: Config): Promise<Client> {
    if (cfg.service === "") throw new ConfigError("Config.service is required");
    if (cfg.version === "")
      throw new ConfigError("Config.version is required; pin it, do not pass a build commit");
    if (cfg.envPrefix === "") throw new ConfigError("Config.envPrefix is required for env mode");
    if (cfg.onUnknown !== Policy.Refuse && cfg.onUnknown !== Policy.Hold)
      throw new ConfigError(
        "Config.onUnknown must be Policy.Refuse (spend gates) or Policy.Hold (instruments); there is no safe default",
      );
    if (cfg.log === undefined)
      throw new ConfigError("Config.log is required; this client's failures are only useful if they are heard");
    if (/^[0-9a-f]{7,40}$/.test(cfg.version))
      throw new ConfigError(
        `Config.version ${JSON.stringify(cfg.version)} looks like a commit hash; the version is the flag contract's, pinned`,
      );
    if (cfg.declared.length === 0)
      throw new ConfigError("Config.declared is empty; a service with no flags has nothing to read");
    if (isAddress(cfg.url))
      throw new ConfigError(`Config.url ${JSON.stringify(cfg.url)} is an address; flipr is reached by name`);

    const client = new Client(cfg, cfg.url.replace(/\/+$/, ""));

    // Declare every flag's series at zero so "nothing has happened" is a
    // readable value rather than a gap. A series that springs into existence on
    // first read cannot be alerted on: a rate() over an absent series is not
    // zero, it is absent, and absent does not fire.
    for (const f of cfg.declared) {
      client.metrics.inc("flipr_client_reads_total", { key: f.key, outcome: "ok" }, 0);
      client.metrics.inc("flipr_client_reads_total", { key: f.key, outcome: "unknown" }, 0);
      if (f.value.kind === "bool")
        client.metrics.gauge("flipr_client_flag_state", { key: f.key }, f.value.value ? 1 : 0);
    }

    if (client.base === "") {
      cfg.log.warn("flipr: no URL configured; env-mode gates", { prefix: cfg.envPrefix });
      client.metrics.gauge("flipr_client_up", {}, 0);
      return client;
    }

    try {
      client.generation = await client.ping();
    } catch (err) {
      // NOT env mode. A flipr that was named and did not answer is an
      // outage, and an outage must not make a service MORE willing to
      // spend: env mode reads the declared defaults, and a declared default
      // can be on. The client stays in flipr mode; every check pings, so
      // the next answer is the promotion; until then the caller's policy
      // applies to every read, and the declarations are published on that
      // first answer.
      cfg.log.error("flipr: configured but unreachable at boot; the policy applies to every read until it answers", {
        url: client.base,
        err: describe(err),
      });
      client.metrics.gauge("flipr_client_up", {}, 0);
      return client;
    }

    client.metrics.gauge("flipr_client_up", {}, 1);
    try {
      await client.publish();
      client.published = true;
      client.metrics.inc("flipr_client_publish_total", { outcome: "ok" }, 1);
    } catch (err) {
      // A refused publish is a CONTRACT failure, not an outage: surface it and
      // stay in flipr mode so the refusal stays visible on every read.
      cfg.log.error("flipr: publish refused", { err: describe(err) });
      client.metrics.inc("flipr_client_publish_total", { outcome: "refused" }, 1);
    }
    cfg.log.info("flipr: connected", {
      namespace: client.namespace(),
      generation: client.generation,
    });
    return client;
  }

  /**
   * Read one flag, with its epistemics separated.
   *
   * `value` is what to act on, already resolved through the policy. `known`
   * says whether anything authoritative answered. `why` is a sentence for a log
   * line.
   */
  async check(key: string): Promise<Reading> {
    if (this.base === "") return this.checkEnv(key);

    // every check pings: the cache is an optimisation on the namespace
    // fetch, never a fallback for flipr's absence
    const down = await this.verify();
    if (down !== undefined) return this.unknown(key, down);

    const fresh = this.cached !== undefined && this.cacheTtlMs > 0 && now() - this.refreshedAt < this.cacheTtlMs;
    if (!fresh) {
      const failed = await this.fetch();
      if (failed !== undefined) return this.unknown(key, failed);
    }

    const flag = this.cached?.flags.find((f) => f.key === key);
    if (flag === undefined) {
      // missing: flipr answered and this key is not declared in the namespace
      return this.unknown(key, `${key} is not declared in ${this.namespace()}`);
    }
    const value = fromWire(flag.value);
    if (value === undefined) {
      // The flag exists in no meaningful sense: the store answered, and the
      // answer had no value in it. Authoritative, but not something to act on.
      this.metrics.inc("flipr_client_reads_total", { key, outcome: "unknown" }, 1);
      return { value: undefined, known: true, why: `${key} has no value in flipr` };
    }

    this.last.set(key, value);
    this.metrics.inc("flipr_client_reads_total", { key, outcome: "ok" }, 1);
    if (value.kind === "bool")
      this.metrics.gauge("flipr_client_flag_state", { key }, value.value ? 1 : 0);

    const why = value.kind === "bool" && !value.value ? `${key} is off in flipr` : "";
    return { value, known: true, why };
  }

  /**
   * Fetch the whole namespace in one trip, pinging first so a store that was
   * wiped or rebuilt is noticed and the declarations republished
   * (collaborative self-heal: publish never clobbers an operator's value).
   * Callers on a loop call this once per interval; check between refreshes
   * costs nothing. Rejects with the reason when nothing authoritative
   * answered.
   */
  async refresh(): Promise<void> {
    const failed = await this.refreshInner();
    if (failed !== undefined) throw new RpcError("refresh", failed);
  }

  /** refresh without the throw: the reason, or undefined on success. */
  private async refreshInner(): Promise<string | undefined> {
    const down = await this.verify();
    if (down !== undefined) return down;
    return this.fetch();
  }

  /**
   * Ping, and on a changed generation drop the cache and republish the
   * declarations (collaborative self-heal: publish never clobbers an
   * operator's value). The reason when flipr did not answer, else undefined.
   */
  private async verify(): Promise<string | undefined> {
    let generation: string;
    try {
      generation = await this.ping();
    } catch (err) {
      return `flipr unreachable (${describe(err)})`;
    }
    const changed = this.generation !== "" && generation !== this.generation;
    const first = !this.published;
    if (changed) {
      this.cached = undefined;
      this.metrics.inc("flipr_client_generation_changes_total", {}, 1);
      this.cfg.log.warn("flipr: store generation changed; republishing declared defaults", {
        generation,
      });
    }
    this.generation = generation;
    if (changed || first) {
      // the declarations reach flipr on the first answer after a boot that
      // found it down, and again after a wipe; never clobbering a value
      try {
        await this.publish();
        this.published = true;
        this.metrics.inc("flipr_client_publish_total", { outcome: "ok" }, 1);
      } catch (err) {
        this.metrics.inc("flipr_client_publish_total", { outcome: "refused" }, 1);
        return `flipr republish failed (${describe(err)})`;
      }
    }
    return undefined;
  }

  /** The namespace in one trip into the cache. The reason on failure, else undefined. */
  private async fetch(): Promise<string | undefined> {
    try {
      const req = create(GetNamespaceRequestSchema, { service: this.cfg.service, version: this.cfg.version });
      const res = await this.rpc("GetNamespace", GetNamespaceRequestSchema, req, GetNamespaceResponseSchema);
      this.cached = res.namespace;
      this.refreshedAt = now();
      return undefined;
    } catch (err) {
      return `flipr GetNamespace ${this.namespace()} failed (${describe(err)})`;
    }
  }

  /**
   * Whether the last answer from flipr was a lock, and why (CONTRACT.md
   * section 9), for the caller's own health line.
   */
  locked(): { locked: boolean; why: string } {
    return { locked: this.lockedWhy !== undefined, why: this.lockedWhy ?? "" };
  }

  /** Every namespace flipr holds. A tool's verb (puffin's audit), never a service's. */
  async list(): Promise<Namespace[]> {
    const res = await this.rpc(
      "ListNamespaces",
      ListNamespacesRequestSchema,
      create(ListNamespacesRequestSchema, {}),
      ListNamespacesResponseSchema,
    );
    return res.namespaces;
  }

  /**
   * The boolean gate: Check with the policy applied and the epistemics dropped.
   *
   * A non-boolean flag read through here is false with a stated reason rather
   * than a coerced truthy value -- silently treating the string "false" as a
   * yes is exactly the class of bug the oneof exists to prevent.
   */
  async enabled(key: string): Promise<{ on: boolean; why: string }> {
    const r = await this.check(key);
    if (r.value === undefined) return { on: false, why: r.why };
    if (r.value.kind !== "bool")
      return { on: false, why: `${key} is a ${r.value.kind} flag, not a gate` };
    return { on: r.value.value, why: r.why };
  }

  /** Read a string flag, or undefined if it is absent or another type. */
  async checkString(key: string): Promise<{ value: string | undefined; known: boolean; why: string }> {
    const r = await this.check(key);
    if (r.value?.kind === "string") return { value: r.value.value, known: r.known, why: r.why };
    return { value: undefined, known: r.known, why: r.why };
  }

  /** Read an integer flag, or undefined if it is absent or another type. */
  async checkInt(key: string): Promise<{ value: bigint | undefined; known: boolean; why: string }> {
    const r = await this.check(key);
    if (r.value?.kind === "int") return { value: r.value.value, known: r.known, why: r.why };
    return { value: undefined, known: r.known, why: r.why };
  }

  /**
   * Fetch the whole namespace in one round trip.
   *
   * The proto describes this as the call a client makes on startup. It is a
   * convenience for a caller rendering a flag table -- a /health handler, a
   * dashboard -- and NOT a way to avoid per-check pings: the doctrine is that
   * every gate read verifies flipr is there, and a cached namespace served
   * without that check is the stale-flag failure wearing a different hat.
   */
  async snapshot(): Promise<Namespace | undefined> {
    if (this.base === "") return undefined;
    if (this.cached === undefined) await this.refreshInner();
    return this.cached;
  }

  /** The store key the gates read, for a /health handler. */
  namespace(): string {
    return `${this.cfg.service}@${this.cfg.version}`;
  }

  /** Which gate regime is live: "flipr" or "env". */
  mode(): "flipr" | "env" {
    return this.base === "" ? "env" : "flipr";
  }

  /** The flags this client publishes, so a caller can build its own series. */
  declared(): readonly Flag[] {
    return this.cfg.declared;
  }

  // -------------------------------------------------------------------------
  // Internals
  // -------------------------------------------------------------------------

  /**
   * Resolve a gate with no flipr.
   *
   * EVERY ANSWER HERE IS KNOWN: the operator's environment is authoritative,
   * including its refusals. Nothing went unanswered; there was simply nothing
   * to ask.
   */
  private checkEnv(key: string): Reading {
    const name = this.envVar(key);
    const raw = this.env[name];
    this.metrics.inc("flipr_client_reads_total", { key, outcome: "env" }, 1);

    if (raw === undefined || raw === "") {
      return { value: this.declaredDefault(key), known: true, why: "" };
    }
    const lowered = raw.toLowerCase();
    if (lowered === "1" || lowered === "true") {
      return { value: boolValue(true), known: true, why: "" };
    }
    if (lowered === "0" || lowered === "false") {
      return { value: boolValue(false), known: true, why: `${key} disabled by ${name}` };
    }

    // A non-boolean declaration reads its environment variable literally: a
    // string flag is exactly the case where "ollama" is the intended value and
    // not a malformed boolean.
    const declared = this.declaredDefault(key);
    if (declared?.kind === "string") {
      return { value: stringValue(raw), known: true, why: "" };
    }
    if (declared?.kind === "int") {
      try {
        return { value: intValue(BigInt(raw)), known: true, why: "" };
      } catch {
        return {
          value: declared,
          known: true,
          why: `${name}=${JSON.stringify(raw)} is not an integer; using the declared default`,
        };
      }
    }

    // The operator wrote it, so it is a KNOWN answer and not an outage -- but it
    // is not a boolean, so fall back to the declaration and say so rather than
    // inventing an interpretation of "yes please".
    return {
      value: declared,
      known: true,
      why: `${name}=${JSON.stringify(raw)} is not a boolean; using the declared default`,
    };
  }

  /** Apply onUnknown to a read that nothing authoritative answered. */
  private unknown(key: string, why: string): Reading {
    this.metrics.inc("flipr_client_reads_total", { key, outcome: "unknown" }, 1);
    if (this.cfg.onUnknown === Policy.Hold) {
      const held = this.last.get(key) ?? this.declaredDefault(key);
      return { value: held, known: false, why: `${why}; holding last known` };
    }
    return { value: boolValue(false), known: false, why: `${why}; gates fail STOP` };
  }

  /** Map smc.enabled to <PREFIX>_SMC_ENABLED, exactly as the Go client does. */
  private envVar(key: string): string {
    // Mirrors Go: strip a trailing ".enabled" and append "_ENABLED", so
    // smc.enabled becomes <PREFIX>_SMC_ENABLED rather than
    // <PREFIX>_SMC_ENABLED_ENABLED. A key that does NOT end in ".enabled" --
    // model.backend, say -- takes the plain form, because appending _ENABLED to
    // a string flag names a variable nobody will ever set.
    const base = key.endsWith(".enabled") ? key.slice(0, -".enabled".length) : key;
    const upper = base.toUpperCase().replace(/[.\-]/g, "_");
    return key.endsWith(".enabled")
      ? `${this.cfg.envPrefix}_${upper}_ENABLED`
      : `${this.cfg.envPrefix}_${upper}`;
  }

  /**
   * A key's declared default, so env mode, the publish and the Hold policy
   * share ONE source of truth about what a flag starts as.
   */
  private declaredDefault(key: string): FlagValue | undefined {
    for (const d of this.cfg.declared) if (d.key === key) return d.value;
    // A key nobody declared is not this service's to authorize.
    return undefined;
  }

  /** The liveness check every read makes; returns the store generation. */
  private async ping(): Promise<string> {
    const res = await this.rpc("Ping", PingRequestSchema, create(PingRequestSchema, {}), PingResponseSchema);
    return res.storeGeneration;
  }

  /** Publish the declared flags; idempotent, never overwrites an operator. */
  private async publish(): Promise<void> {
    const namespace = create(NamespaceSchema, {
      service: this.cfg.service,
      version: this.cfg.version,
      flags: this.cfg.declared.map((d) =>
        create(FlagSchema, {
          key: d.key,
          value: toWire(d.value),
          description: d.desc,
          expensive: d.expensive,
        }),
      ),
    });
    const req = create(PublishNamespaceRequestSchema, { namespace });
    await this.rpc("PublishNamespace", PublishNamespaceRequestSchema, req, PublishNamespaceResponseSchema);
  }

  /**
   * POST protojson to one FliprService method and decode the reply.
   *
   * protojson over plain HTTP, not gRPC, because
   * gRPC would drag HTTP/2 and generated stubs into every consumer for a
   * service whose entire job is answering a question that fits in a cache line.
   */
  private async rpc<Req extends Message, Res extends Message>(
    method: string,
    reqSchema: GenMessage<Req>,
    req: Req,
    resSchema: GenMessage<Res>,
  ): Promise<Res> {
    const started = now();
    let lastError: unknown;
    try {
      // a remembered lock answers at once, with no request, until its
      // deadline (CONTRACT.md section 9): a hot path cannot storm a locked flipr
      if (this.lockedWhy !== undefined && now() < this.lockUntil) {
        this.metrics.inc("flipr_client_requests_total", { method, outcome: "locked" }, 1);
        throw new RpcError(method, `423: ${this.lockedWhy} (remembered)`, 423, { reason: this.lockedWhy });
      }
      for (let attempt = 0; attempt < this.retry.attempts; attempt++) {
        if (attempt > 0) {
          let delay = backoffMs(attempt - 1, this.retry, this.random);
          // a locked or rate-limited flipr says how long to wait; honour it,
          // capped at the lock ceiling, so the client neither storms nor stalls
          if (lastError instanceof RpcError && lastError.retryAfterMs !== undefined)
            delay = Math.min(lastError.retryAfterMs, LOCK_CEILING_MS);
          this.metrics.inc("flipr_client_retries_total", { method }, 1);
          await this.sleep(delay);
        }
        try {
          const res = await this.attempt(method, reqSchema, req, resSchema);
          if (this.lockedWhy !== undefined) this.cfg.log.info("flipr: unlocked", { was: this.lockedWhy });
          this.lockedWhy = undefined;
          this.lockUntil = -Infinity;
          this.metrics.inc("flipr_client_requests_total", { method, outcome: "value" }, 1);
          return res;
        } catch (err) {
          lastError = err;
          if (err instanceof RpcError && err.status === 423) {
            const holdMs = Math.min(err.retryAfterMs ?? LOCK_DEFAULT_MS, LOCK_CEILING_MS);
            if (this.lockedWhy === undefined)
              this.cfg.log.warn("flipr: locked; holding and backing off", { reason: err.reason, holdMs });
            this.lockedWhy = err.reason;
            this.lockUntil = now() + holdMs;
          }
          // A contract failure is not a transient. Asking the same wrong
          // question three times just spends the budget and delays the error.
          if (!isRetryable(err)) break;
        }
      }
      this.metrics.inc("flipr_client_requests_total", { method, outcome: outcomeOf(lastError) }, 1);
      throw lastError instanceof RpcError ? lastError : new RpcError(method, describe(lastError));
    } finally {
      this.metrics.observe("flipr_client_duration_seconds", { method }, (now() - started) / 1000);
    }
  }

  /** One try: POST protojson to a method and decode the reply. */
  private async attempt<Req extends Message, Res extends Message>(
    method: string,
    reqSchema: GenMessage<Req>,
    req: Req,
    resSchema: GenMessage<Res>,
  ): Promise<Res> {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeoutMs);
    try {
      const response = await this.fetchImpl(`${this.base}/flipr.v1.FliprService/${method}`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          // Self-declared attribution for flipr's oplog. This client reads and
          // publishes its own declarations; it never calls SetFlag. Saying so
          // on every call means whoever mines the oplog for who touched a
          // namespace can rule it out at a glance instead of wondering.
          "X-Flipr-Caller": this.namespace(),
          // And which client this is, so a copy announces itself as a copy
          // (CONTRACT.md section 8).
          "X-Flipr-Client": clientHeader(),
        },
        body: JSON.stringify(toJson(reqSchema, req)),
        signal: controller.signal,
      });

      // Bounded read. An unbounded json() on a flipr that is confused or
      // hostile is an unbounded allocation in every consumer of this library.
      const raw = await readBounded(response, 1 << 22);
      if (!response.ok) {
        const body = raw.trim();
        const fromFlipr = (response.headers.get("content-type") ?? "").startsWith("application/json") || body.startsWith("{");
        const ra = Number(response.headers.get("retry-after") ?? "");
        let reason = body;
        try {
          const parsed = JSON.parse(body) as { error?: string; reason?: string };
          reason = parsed.error ?? parsed.reason ?? body;
        } catch {
          // not JSON: the edge's text, kept as is
        }
        const opts: { retryAfterMs?: number; fromFlipr?: boolean; reason?: string } = { fromFlipr, reason };
        if (Number.isFinite(ra) && ra > 0) opts.retryAfterMs = ra * 1000;
        throw new RpcError(method, `${response.status}: ${body}`, response.status, opts);
      }
      return fromJson(resSchema, JSON.parse(raw));
    } catch (err) {
      throw err instanceof RpcError ? err : new RpcError(method, describe(err));
    } finally {
      clearTimeout(timer);
    }
  }
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

/** Monotonic-ish milliseconds, without requiring a DOM lib. */
function now(): number {
  return typeof performance !== "undefined" ? performance.now() : Date.now();
}

/** Render any thrown thing as a sentence fragment for a log line. */
function describe(err: unknown): string {
  if (err instanceof Error) return err.message;
  return String(err);
}

/**
 * Read a response body, refusing anything past `limit` bytes.
 *
 * The Go client does this with io.LimitReader. Doing it here rather than
 * calling response.json() means a flipr returning something enormous is a
 * stated error rather than a consumer's heap.
 */
async function readBounded(response: Response, limit: number): Promise<string> {
  const text = await response.text();
  if (text.length > limit) throw new RpcError("read", `response exceeded ${limit} bytes`);
  return text;
}
