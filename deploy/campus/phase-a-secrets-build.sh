#!/usr/bin/env bash
# Campus Phase A: secrets + image build/import + apply EE overlay.
# Never prints secret values. Run on stjosephs as aethercode.
set -euo pipefail
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
LOG=/data/production/aethercode-execution-engine/DEPLOY-LOG.md
REPO=/data/development/aethercode-execution-engine
ts=$(TZ=Asia/Kolkata date '+%Y-%m-%d %H:%M:%S IST')

cd "$REPO"
git fetch origin production
git checkout production
git reset --hard origin/production
SHA=$(git rev-parse --short=12 HEAD)

PW_MIG=$(openssl rand -base64 36 | tr -d '/+=' | head -c 40)
PW_APP=$(openssl rand -base64 36 | tr -d '/+=' | head -c 40)
JUDGE_TOKEN=$(openssl rand -base64 36 | tr -d '/+=' | head -c 40)
REDIS_PW=$(openssl rand -base64 36 | tr -d '/+=' | head -c 40)

sudo docker exec -u postgres postgres-main-postgres-1 \
  psql -v ON_ERROR_STOP=1 \
  -c "ALTER ROLE aether_exec_migrator PASSWORD '${PW_MIG}';" \
  -c "ALTER ROLE aether_exec_app PASSWORD '${PW_APP}';" >/dev/null

QC=$(sudo docker exec -u postgres postgres-main-postgres-1 \
  psql -d question_intake -tAc "select count(*) from questions")
test "$QC" = "115"

SEC=$(mktemp -d)
chmod 700 "$SEC"
umask 077
printf '%s' "postgresql://aether_exec_app:${PW_APP}@pg-main:5432/aethercode_exec?sslmode=prefer" >"$SEC/AC_DATABASE_URL"
printf '%s' "$JUDGE_TOKEN" >"$SEC/AC_JUDGE_TOKEN"
printf '%s' "$REDIS_PW" >"$SEC/AC_REDIS_PASSWORD"
printf '%s' "aethercode-exec-redis.aethercode.svc.cluster.local:6379" >"$SEC/AC_REDIS_ADDR"
printf '%s' "false" >"$SEC/AC_AUTH_ENABLED"
printf '%s' "true" >"$SEC/AC_ALLOW_UNAUTH"
printf '%s' "http://go-judge-svc.aethercode.svc.cluster.local:5050" >"$SEC/AC_JUDGE_URL"

sudo k3s kubectl -n aethercode delete secret aethercode-exec-secrets --ignore-not-found >/dev/null
sudo k3s kubectl -n aethercode create secret generic aethercode-exec-secrets --from-file="$SEC" >/dev/null

sudo k3s kubectl -n aethercode delete secret aethercode-exec-migrator --ignore-not-found >/dev/null
sudo k3s kubectl -n aethercode create secret generic aethercode-exec-migrator \
  --from-literal=AC_DATABASE_URL="postgresql://aether_exec_migrator:${PW_MIG}@pg-main:5432/aethercode_exec?sslmode=prefer" >/dev/null

# Persist judge token only for this build session file (shredded after import)
printf '%s' "$JUDGE_TOKEN" >/data/production/aethercode-execution-engine/.judge_token
chmod 600 /data/production/aethercode-execution-engine/.judge_token

find "$SEC" -type f -exec shred -u {} + 2>/dev/null || rm -rf "$SEC"
rmdir "$SEC" 2>/dev/null || rm -rf "$SEC"
unset PW_MIG PW_APP JUDGE_TOKEN REDIS_PW

{
  echo
  echo "## $ts — secrets + build (SHA $SHA)"
  echo "- question_intake.questions=$QC"
  echo "- secrets aethercode-exec-secrets / aethercode-exec-migrator created"
} >>"$LOG"

echo "secrets_ok"

export DOCKER_BUILDKIT=1
echo "building_engine"
docker build -t "aethercode/exec-engine:${SHA}" -f Dockerfile .
echo "building_go_judge"
docker build -t "aethercode/go-judge:${SHA}" -f docker/go-judge/Dockerfile docker/go-judge
docker pull redis:7.4-alpine

echo "importing_images"
docker save "aethercode/exec-engine:${SHA}" | sudo k3s ctr -n k8s.io images import -
docker save "aethercode/go-judge:${SHA}" | sudo k3s ctr -n k8s.io images import -
docker save redis:7.4-alpine | sudo k3s ctr -n k8s.io images import -

# Point overlay at built tags
cd "$REPO/deploy/k8s-overlays/campus-production"
sed -i "s/newTag: .*/newTag: ${SHA}/" kustomization.yaml || true
# Ensure both images use SHA (kustomization has two newTag lines)
perl -i -0pe "s/newName: aethercode\\/exec-engine\\n\\s*newTag: \\S+/newName: aethercode\\/exec-engine\\n    newTag: ${SHA}/g; s/newName: aethercode\\/go-judge\\n\\s*newTag: \\S+/newName: aethercode\\/go-judge\\n    newTag: ${SHA}/g" kustomization.yaml

cd "$REPO"
echo "applying_overlay"
sudo k3s kubectl apply --server-side -k deploy/k8s-overlays/campus-production

echo "waiting_rollout"
sudo k3s kubectl -n aethercode rollout status deploy/aethercode-exec --timeout=300s || true
sudo k3s kubectl -n aethercode get pods,svc,ds,sts -o wide

QC2=$(sudo docker exec -u postgres postgres-main-postgres-1 \
  psql -d question_intake -tAc "select count(*) from questions")
{
  echo "- images aethercode/exec-engine:${SHA} aethercode/go-judge:${SHA} imported"
  echo "- overlay applied; question_intake.questions=$QC2"
} >>"$LOG"

shred -u /data/production/aethercode-execution-engine/.judge_token 2>/dev/null || \
  rm -f /data/production/aethercode-execution-engine/.judge_token

echo "done sha=$SHA questions=$QC2"
