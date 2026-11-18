#!/bin/sh
# KAFKA_PASSWORD_FILE·DB_PASSWORD_FILE이 있으면 파일 내용을 환경 변수로 읽는다
# (docker inspect에 비밀번호가 보이지 않게 하려고). 설정 검증은 서버가 한다
set -eu

if [ -n "${KAFKA_PASSWORD_FILE:-}" ]; then
  KAFKA_PASSWORD="$(cat "$KAFKA_PASSWORD_FILE")"
  export KAFKA_PASSWORD
  unset KAFKA_PASSWORD_FILE
fi
if [ -n "${DB_PASSWORD_FILE:-}" ]; then
  DB_PASSWORD="$(cat "$DB_PASSWORD_FILE")"
  export DB_PASSWORD
  unset DB_PASSWORD_FILE
fi

exec "$@"
