#!/usr/bin/env bash
# AI 서버 test 배포 (WSL, 저장소 루트에서). 커밋된 코드만 묶어 서버로 보내고 서버에서 빌드한다
#
#   deploy/test/deploy.sh build     # HEAD 묶음 → scp → 서버에서 docker build (실행 중인 서버는 그대로)
#   deploy/test/deploy.sh check     # 마지막 빌드 이미지로 Kafka 접속 점검 (읽기 전용)
#   deploy/test/deploy.sh migrate   # 마지막 빌드 이미지로 DB 마이그레이션
#   deploy/test/deploy.sh run       # 기존 컨테이너를 멈추고 마지막 빌드 이미지로 실행
#
# check·migrate·run은 로컬 HEAD가 아니라 서버의 마지막 빌드($BASE/built_rev)를 쓴다
#   deploy/test/deploy.sh status    # 컨테이너 상태·최근 로그
#
# 서버 접속 이름은 AI_HOST (기본 runiverse-ai-test, ~/.ssh/config에서 배스천 경유)
set -euo pipefail

HOST="${AI_HOST:-runiverse-ai-test}"
BASE=/opt/runiverse-ai-server
NAME=runiverse-ai-server
REV="$(git rev-parse --short=12 HEAD)"
IMAGE="runiverse-ai-server:$REV"

# 서버에서 실행할 docker run 공통 옵션 (설정 파일 + 비밀 파일 읽기 전용)
# 서버의 마지막 빌드 이미지 (원격 셸에서 펼쳐짐)
BUILT="runiverse-ai-server:\$(cat $BASE/built_rev)"

RUN_OPTS="--env-file $BASE/config/server.env -v $BASE/secrets:/run/secrets/ai-server:ro"

remote() { ssh "$HOST" "set -euo pipefail; $1"; }

case "${1:-}" in
  build)
    if ! git diff --quiet HEAD --; then
      echo "⚠️ 커밋 안 한 변경이 있습니다. 묶음에는 HEAD($REV)만 들어갑니다" >&2
    fi
    archive="$(mktemp --suffix=.tar.gz)"
    git archive --format=tar.gz -o "$archive" HEAD
    scp -q "$archive" "$HOST:/tmp/ai-server-$REV.tar.gz"
    rm -f "$archive"
    remote "
      sudo mkdir -p $BASE/src/$REV
      sudo tar -xzf /tmp/ai-server-$REV.tar.gz -C $BASE/src/$REV
      rm -f /tmp/ai-server-$REV.tar.gz
      sudo docker build -q -t $IMAGE $BASE/src/$REV
      echo $REV | sudo tee $BASE/built_rev > /dev/null
      echo '✅ 빌드 $IMAGE'"
    ;;
  check)
    remote "echo \"이미지 $BUILT\"; sudo docker run --rm $RUN_OPTS $BUILT python scripts/kafka_check.py"
    ;;
  migrate)
    remote "echo \"이미지 $BUILT\"; sudo docker run --rm $RUN_OPTS $BUILT python -m runiverse_ai_server.migrate"
    ;;
  run)
    remote "
      sudo docker image inspect $BUILT > /dev/null
      sudo docker rm -f $NAME > /dev/null 2>&1 || true
      sudo docker run -d --name $NAME --restart unless-stopped --stop-timeout 30 \
        --log-opt max-size=10m --log-opt max-file=5 \
        $RUN_OPTS $BUILT > /dev/null
      sleep 5
      sudo docker ps --filter name=$NAME --format '{{.Names}} {{.Image}} {{.Status}}'
      sudo docker logs --tail 5 $NAME"
    ;;
  status)
    remote "
      sudo docker ps -a --filter name=$NAME --format '{{.Names}} {{.Image}} {{.Status}}'
      sudo docker logs --tail 20 $NAME"
    ;;
  *)
    sed -n '2,12p' "$0"
    exit 2
    ;;
esac
