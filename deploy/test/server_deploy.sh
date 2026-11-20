#!/usr/bin/env bash
# CD(deploy-test.yml)가 SSM Run Command로 AI 서버에서 root로 실행한다
#   IMAGE=<ECR 레지스트리>/runiverse-ai-server:<커밋 12자> bash server_deploy.sh
#
# 출력은 공개 저장소의 Actions 로그에 남는다 → 서버 로그·설정 값·오류 원문을 출력하지 않는다.
# 실패 원인은 서버에서 $BASE/deploy-logs/ 또는 deploy.sh status로 본다
set -euo pipefail

: "${IMAGE:?IMAGE가 필요합니다}"
BASE="${BASE:-/opt/runiverse-ai-server}"
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

start() {  # 기존 컨테이너를 지우고 $1 이미지로 띄운다
  docker rm -f "$NAME" > /dev/null 2>&1 || true
  docker run -d --name "$NAME" --restart unless-stopped --stop-timeout 30 \
    --log-opt max-size=10m --log-opt max-file=5 \
    "${RUN_OPTS[@]}" "$1" > /dev/null
}

started() {  # 서버가 started 이벤트를 남길 때까지 최대 60초
  for _ in $(seq 1 12); do
    # grep -q는 먼저 끝나 docker logs가 SIGPIPE → pipefail에서 실패로 보임. 끝까지 읽는다
    docker logs "$NAME" 2>&1 | grep '"event": "started"' > /dev/null && return 0
    sleep 5
  done
  return 1
}

# 되돌릴 이미지 = 지금 실행 중인 컨테이너의 이미지 (처음 배포면 없음)
PREVIOUS="$(docker inspect -f '{{.Config.Image}}' "$NAME" 2>/dev/null || true)"

start "$IMAGE"
if started; then
  echo "$TAG" > "$BASE/deployed_tag"
  echo "✅ 실행 $TAG"
  docker image prune -a -f > /dev/null 2>&1 || true  # 실행 중이 아닌 이미지 정리 (ECR에 남아 있음)
  exit 0
fi

docker logs "$NAME" > "$LOGS/failed-start.log" 2>&1 || true
echo "❌ 시작 확인 실패 $TAG ($LOGS/failed-start.log)"
if [ -z "$PREVIOUS" ] || [ "$PREVIOUS" = "$IMAGE" ]; then
  echo "⚠️ 되돌릴 이전 이미지 없음 · 서버 중지 상태"
  docker rm -f "$NAME" > /dev/null 2>&1 || true
  exit 1
fi
start "$PREVIOUS"
if started; then
  echo "↩️ 이전 이미지로 되돌림 ${PREVIOUS##*:}"
else
  echo "⚠️ 되돌림도 시작 확인 실패 ${PREVIOUS##*:} · deploy.sh status로 확인"
fi
exit 1
