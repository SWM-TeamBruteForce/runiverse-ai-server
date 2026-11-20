#!/usr/bin/env bash
# CD(deploy-test.yml)가 SSM Run Command로 AI 서버에서 root로 실행한다
#   IMAGE=<ECR 레지스트리>/runiverse-ai-server:<커밋 12자> bash server_deploy.sh
#
# 출력은 공개 저장소의 Actions 로그에 남는다 → 서버 로그·설정 값·오류 원문을 출력하지 않는다.
# 실패 원인은 서버에서 $BASE/deploy-logs/ 또는 deploy.sh status로 본다
set -euo pipefail

: "${IMAGE:?IMAGE가 필요합니다}"
BASE=/opt/runiverse-ai-server
NAME=runiverse-ai-server
TAG="${IMAGE##*:}"
LOGS="$BASE/deploy-logs"
RUN_OPTS=(--env-file "$BASE/config/server.env" -v "$BASE/secrets:/run/secrets/ai-server:ro")

# SSM은 HOME 없이 실행될 수 있다 → docker·ecr-login이 root 설정(/root/.docker, /root/.ecr)을 쓰게 한다
export HOME="${HOME:-/root}" DOCKER_CONFIG="${DOCKER_CONFIG:-/root/.docker}"

mkdir -p "$LOGS" && chmod 700 "$LOGS"

# ECR 로그인은 docker 자격 증명 도우미(ecr-login)가 인스턴스 역할로 한다
if ! docker pull -q "$IMAGE" > "$LOGS/pull.log" 2>&1; then
  echo "❌ 이미지 받기 실패 ($LOGS/pull.log)"; exit 1
fi
echo "✅ 이미지 $TAG"

if ! docker run --rm "${RUN_OPTS[@]}" "$IMAGE" python -m runiverse_ai_server.migrate > "$LOGS/migrate.log" 2>&1; then
  echo "❌ 마이그레이션 실패 ($LOGS/migrate.log) · 실행 중인 서버는 그대로"; exit 1
fi
echo "✅ 마이그레이션 · $(tail -n 1 "$LOGS/migrate.log")"

docker rm -f "$NAME" > /dev/null 2>&1 || true
docker run -d --name "$NAME" --restart unless-stopped --stop-timeout 30 \
  --log-opt max-size=10m --log-opt max-file=5 \
  "${RUN_OPTS[@]}" "$IMAGE" > /dev/null

# 시작 확인: 서버가 started 이벤트를 남길 때까지 최대 60초
for _ in $(seq 1 12); do
  if docker logs "$NAME" 2>&1 | grep -q '"event": "started"'; then
    echo "$TAG" > "$BASE/deployed_tag"
    echo "✅ 실행 $TAG"
    exit 0
  fi
  sleep 5
done
echo "❌ 시작 확인 실패 · 서버에서 deploy.sh status로 로그 확인"
exit 1
