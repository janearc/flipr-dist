# Client changelog

One line per tag, the contract section it touches, and what a consumer
does. Tags: `clients/go/vX.Y.Z`, `client-ts/vX.Y.Z`, `client-py/vX.Y.Z`.

## Go 1.1.1 (2026-09-06)

- The stamp is committed (`stamp.go`, section 13): a consumer that pins the
  tag announces `go/v1.1.1/<hash>` with nothing passed at build. Before this
  the tag and hash came only from `-ldflags`, which no consumer passed, so
  every Go consumer announced `go/dev/unknown` and counted as `other`.
- A landed `Set` drops the client's cached namespace, so a tool that flips
  and reads back sees the new value at once instead of the cache for a TTL.
- Consumers: bump the pin; nothing else changes. TypeScript and Python stay
  at 1.1.0 (their stamps were committed from the start).

## 1.1.0, all three (2026-09-06)

- A configured flipr that does not answer at boot is not env mode: the
  policy applies until it answers, and the declarations are published on
  the first answer (section 5). Go `Up()`; Python first-answer publish.
- Tags moved once before anything pinned them, after the Python suite ran
  against a live flipr; the rule since: full suites against a live flipr
  before a tag, and a tag is never moved once pinned.

## 1.0.0, all three (2026-09-06)

- The contract (`CONTRACT.md`): identity headers, five verbs, outcomes,
  policy with no default, every check pings, retry with full jitter, the
  remembered lock, instruments, refusals at construction, the stamp.
