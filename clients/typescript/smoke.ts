// End-to-end smoke against a LIVE flipr.
//
// The unit suite stubs the transport, which proves the logic. This proves the
// client speaks to the flipr that actually exists: real protojson, real
// generation, real publish.
//
// IT CLEANS UP AFTER ITSELF. Every deploy minting a namespace is how the store
// accumulated 21 dead dodo rows in one evening, so this publishes under a
// throwaway name and then retires it. The delete is a curl and not a client
// method on purpose: DeleteNamespace is an OPERATOR operation carrying a
// stated reason, and this script is acting as the operator.
//
//   npx tsx smoke.ts [url]

import { Client, Policy, boolValue, formatValue, stringValue, type Flag } from "./flipr.js";

const url = process.argv[2] ?? "http://flipr.test";
const SERVICE = "flipr-ts-client-smoke";
const VERSION = "smoke";

const declared: Flag[] = [
  { key: "gate.enabled", value: boolValue(false), desc: "on: the smoke pretends to spend. off: it does not. Test fixture; safe to delete.", expensive: true },
  { key: "model.backend", value: stringValue("ollama"), desc: "the string case the Go client cannot express. Test fixture; safe to delete.", expensive: false },
];

const log = {
  info: (m: string, f?: Record<string, unknown>) => console.log(`  info  ${m} ${f ? JSON.stringify(f) : ""}`),
  warn: (m: string, f?: Record<string, unknown>) => console.log(`  warn  ${m} ${f ? JSON.stringify(f) : ""}`),
  error: (m: string, f?: Record<string, unknown>) => console.log(`  ERROR ${m} ${f ? JSON.stringify(f) : ""}`),
};

let failures = 0;

/** Assert and report, without stopping at the first failure. */
function check(name: string, got: unknown, want: unknown): void {
  const ok = JSON.stringify(got) === JSON.stringify(want);
  console.log(`  ${ok ? "ok  " : "FAIL"}  ${name}: ${JSON.stringify(got)}${ok ? "" : ` (wanted ${JSON.stringify(want)})`}`);
  if (!ok) failures++;
}

/** Retire the throwaway namespace, with the reason the proto requires. */
async function retire(): Promise<void> {
  const res = await fetch(`${url}/flipr.v1.FliprService/DeleteNamespace`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "X-Flipr-Caller": `${SERVICE}@${VERSION}` },
    body: JSON.stringify({
      service: SERVICE,
      version: VERSION,
      reason: "flipr TypeScript client smoke test cleaning up its own fixture namespace",
    }),
  });
  console.log(`  cleanup: DeleteNamespace -> ${res.status} ${(await res.text()).trim()}`);
}

async function main(): Promise<void> {
  console.log(`smoke: TypeScript flipr client against ${url}`);
  const c = await Client.connect({
    service: SERVICE,
    version: VERSION,
    url,
    envPrefix: "SMOKE",
    declared,
    onUnknown: Policy.Refuse,
    log,
  });

  try {
    check("mode", c.mode(), "flipr");
    check("namespace", c.namespace(), `${SERVICE}@${VERSION}`);

    // The boolean gate, declared off.
    const gate = await c.enabled("gate.enabled");
    check("gate.enabled is off", gate.on, false);

    // The string flag: the whole reason this client models the proto rather
    // than the Go client.
    const backend = await c.checkString("model.backend");
    check("model.backend reads as a string", backend.value, "ollama");
    check("model.backend is known", backend.known, true);

    // The namespace round trip.
    const ns = await c.snapshot();
    check("snapshot returns both flags", ns?.flags.length, 2);
    for (const f of ns?.flags ?? []) {
      console.log(`        ${f.key} = ${formatValue(f.value?.kind.case === "boolValue" ? { kind: "bool", value: f.value.kind.value } : f.value?.kind.case === "stringValue" ? { kind: "string", value: f.value.kind.value } : undefined)}`);
    }

    // An undeclared key is never on.
    check("undeclared key refuses", (await c.enabled("not.declared")).on, false);
  } finally {
    await retire();
  }

  console.log(failures === 0 ? "smoke: PASS" : `smoke: FAIL (${failures})`);
  process.exit(failures === 0 ? 0 : 1);
}

void main();
