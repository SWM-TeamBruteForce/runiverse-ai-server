# Runiverse AI 서버 (test 서버 EC2에서 실행). 설정은 환경 변수로만, 비밀번호는 *_FILE로 파일 전달 가능
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 PYTHONUTF8=1 PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1
WORKDIR /app

COPY pyproject.toml ./
COPY src ./src
RUN pip install . && rm -rf src build

# migrate는 실행 디렉터리의 migrations/를 읽는다. scripts/는 컨테이너 안 Kafka 점검용
COPY migrations ./migrations
COPY scripts/kafka_check.py ./scripts/
COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh

RUN useradd --system --uid 10001 --no-create-home app
USER 10001

ENTRYPOINT ["entrypoint.sh"]
CMD ["python", "-m", "runiverse_ai_server.main"]
