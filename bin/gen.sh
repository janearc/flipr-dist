#!/bin/sh
# regenerate wire code from the protos, for ALL THREE modules:
#   gen/fliprpb                     the server's types (this module)
#   clients/go/flipr/v1             the standalone Go client module's types
#   clients/typescript/flipr/v1     the standalone TS client package's types
#
# gen-freshness rule: run this, and the tree must be clean afterwards.
#
# THE PIN IS ENFORCED, NOT SUGGESTED. protoc-gen-go stamps its own version into
# every generated file, so a plugin a minor version off rewrites every file and
# breaks gen-freshness for reasons that have nothing to do with the contract.
# Installing to a REPO-LOCAL bin rather than $HOME/go/bin is deliberate: other
# repos in this estate pin different versions of the same plugin, and a shared
# $HOME/go/bin makes whichever one ran last the winner. A `command -v` guard
# does not pin anything -- it silently accepts whatever is already installed,
# which is how this script's first draft produced v1.36.6 output in a v1.36.12
# tree.
set -e
cd "$(dirname "$0")/.."

# Matches google.golang.org/protobuf in go.mod. Move them together.
PROTOC_GEN_GO_VERSION=v1.36.12
TOOLS="$PWD/.tools"

# The TypeScript plugin is pinned the same way and for the same reason, but by
# npm rather than by go install: protoc-gen-es stamps its version into every
# generated file, so an unpinned plugin rewrites the tree and breaks
# gen-freshness for reasons unrelated to the contract. `npm ci` installs
# EXACTLY the committed lockfile and fails if package.json disagrees with it,
# which is the npm equivalent of the version check below. Installing into the
# package's own node_modules keeps it repo-local, like .tools/.
TS_CLIENT="$PWD/clients/typescript"

if [ "$("$TOOLS/protoc-gen-go" --version 2>/dev/null)" != "protoc-gen-go $PROTOC_GEN_GO_VERSION" ]; then
  echo "installing protoc-gen-go $PROTOC_GEN_GO_VERSION into .tools"
  GOPRIVATE=github.com/janearc GOBIN="$TOOLS" \
    go install "google.golang.org/protobuf/cmd/protoc-gen-go@$PROTOC_GEN_GO_VERSION"
fi

if [ ! -x "$TS_CLIENT/node_modules/.bin/protoc-gen-es" ]; then
  echo "installing the TypeScript client's pinned toolchain"
  npm ci --prefix "$TS_CLIENT" --silent
fi

export PATH="$TOOLS:$PATH"
buf lint
buf generate
# Both clients speak FliprService only. oplog is flipr's internal audit record
# and no consumer reads it, so it stays out of the client modules.
buf generate --template buf.gen.client.yaml --path proto/flipr/v1/flipr.proto
buf generate --template buf.gen.ts-client.yaml --path proto/flipr/v1/flipr.proto
# The contract flipr serves at /api, embedded in the binary. It is generated
# here with everything else, so the contract served can never fall behind
# the protos the way a hand-built file once did.
buf build -o descriptor.binpb
