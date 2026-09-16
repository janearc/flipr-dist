#!/bin/bash
# deploy.sh <environment> -- build flipr at the committed tree, import it into
# that environment's cluster, and roll it out. The ONE way flipr reaches a
# cluster, until deployd takes this script's job.
#
# THE ENVIRONMENT IS THE ONLY INPUT. Everything else about the target, the
# cluster, the kubectl context, the edge port, the data root, comes from
# kube/environments/<environment>.env, so this script cannot assume one
# cluster while acting on another. Before it writes anything it
# says which cluster it is about to act on and refuses if the context named by
# the file is not the one kubectl would use.
#
# The image tag is the COMMIT HASH, taken from git, never typed by hand, and
# the same hash is substituted into the deployment manifest at apply, with the
# environment's data root and name substituted beside it, all as ${VAR}, the
# estate's one placeholder syntax. The manifests in the tree carry
# placeholders, never a literal tag or path.
#
# Refuses a dirty tree. An image built from uncommitted changes carries a hash
# that lies about its contents, which is worse than no version at all.

set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
ENVNAME="${1:-}"
if [ -z "$ENVNAME" ]; then
  echo "usage: bin/deploy.sh <environment>   (one of: $(ls "$REPO/kube/environments" | sed 's/\.env$//' | tr '\n' ' '))" >&2
  exit 2
fi
ENVFILE="$REPO/kube/environments/$ENVNAME.env"
if [ ! -f "$ENVFILE" ]; then
  echo "deploy: no such environment: $ENVFILE" >&2
  exit 2
fi
# shellcheck disable=SC1090
. "$ENVFILE"
for v in ENV CLUSTER CTX EDGE_PORT DATA_ROOT; do
  [ -n "${!v:-}" ] || { echo "deploy: $ENVFILE does not set $v" >&2; exit 2; }
done

cd "$REPO"

if ! git -C "$REPO" diff --quiet || ! git -C "$REPO" diff --cached --quiet; then
  echo "deploy: the tree is dirty. Commit first -- the image tag is the commit hash," >&2
  echo "deploy: and a hash over uncommitted changes lies about what it contains." >&2
  exit 1
fi

# THE TARGET, STATED AND CHECKED before any write. The context must exist and
# must be the one the cluster derives; the data root must already hold the
# flipr directory, because the volume is type Directory on purpose and a pod
# that could create it would start clean and serve nothing while looking
# healthy.
echo "== target: environment $ENV, cluster $CLUSTER, context $CTX, edge port $EDGE_PORT, data root $DATA_ROOT =="
if ! kubectl config get-contexts -o name | grep -qx "$CTX"; then
  echo "deploy: context $CTX is not in the kubeconfig; refusing" >&2
  exit 1
fi
if [ "$CTX" != "k3d-$CLUSTER" ]; then
  echo "deploy: $ENVFILE names context $CTX for cluster $CLUSTER; k3d derives k3d-$CLUSTER; refusing a target that does not agree with itself" >&2
  exit 1
fi
if [ ! -d "$DATA_ROOT/flipr" ]; then
  echo "deploy: $DATA_ROOT/flipr does not exist on the host. The flags volume is type Directory by design;" >&2
  echo "deploy: create it (and $DATA_ROOT/backups/flipr) before deploying flipr into $ENV" >&2
  exit 1
fi
mkdir -p "$DATA_ROOT/backups/flipr"

SHA="$(git -C "$REPO" rev-parse --short=7 HEAD)"
IMG="flipr:$SHA"

# substitute renders one manifest with the placeholders filled: the commit
# hash, the data root and the environment label.
substitute() {
  sed -e "s/\${COMMIT}/$SHA/g" -e "s#\${DATA_ROOT}#$DATA_ROOT#g" -e "s/\${ENV}/$ENV/g" "$1"
}

echo "== building $IMG (tests run inside the build under the race detector; a failing suite refuses the image) =="
docker build --build-arg COMMIT="$SHA" -t "$IMG" "$REPO"

echo "== importing into $CLUSTER =="
k3d image import "$IMG" -c "$CLUSTER"

echo "== applying manifests to $CTX =="
for f in 00-namespace 10-pvc 20-deployment 30-service 40-ingressroute 50-export-cronjob; do
  substitute "$REPO/kube/$f.yaml" | kubectl --context "$CTX" apply -f -
done

echo "== waiting for rollout (Recreate: the old pod stops first, by design) =="
kubectl --context "$CTX" -n flipr rollout status deployment/flipr --timeout=180s

echo "== dashboard: push to this environment's grafana (idempotent; overwrite by uid) =="
# READ THE BODY, NOT THE EXIT STATUS: traefik 404s "successfully" when grafana
# is absent-but-routed, and curl calls that a success.
GRAFANA="${GRAFANA_URL:-http://grafana.test:$EDGE_PORT}"
health="$(curl -s --max-time 5 "$GRAFANA/api/health" || true)"
if printf '%s' "$health" | grep -q '"database"'; then
  # the dashboard carries ${ENV} like the manifests, so its uid is
  # flipr-<environment> and it reads that environment's prometheus.
  payload="$(substitute "$REPO/kube/dashboards/flipr.json" | python3 -c '
import json, sys
print(json.dumps({"dashboard": json.load(sys.stdin), "overwrite": True,
                  "message": "deployed by flipr bin/deploy.sh"}))
')"
  out="$(printf '%s' "$payload" | curl -s --max-time 10 -X POST -H 'Content-Type: application/json' -d @- "$GRAFANA/api/dashboards/db" || true)"
  if printf '%s' "$out" | grep -q '"status":"success"'; then
    echo "dashboard: flipr-$ENV pushed"
  else
    echo "dashboard: PUSH FAILED, deploy continues (grafana said: $out)" >&2
  fi
else
  echo "dashboard: no grafana answering as grafana in $ENV; skipped (said: ${health:-nothing})" >&2
fi

echo "== verify: the service answers by NAME, through this environment's edge, with the version this tree built =="
sleep 1
got="$(curl -s --max-time 5 "http://flipr.test:${EDGE_PORT}/health" || true)"
echo "$got"
if ! printf '%s' "$got" | grep -q "\"version\":\"$SHA\""; then
  echo "deploy: flipr.test:$EDGE_PORT does not report $SHA; the roll finished but the edge does not show it" >&2
  exit 1
fi
echo "deployed $IMG to $ENV"
