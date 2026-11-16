"""AI 서버 진입점: 설정 → 어댑터 조립 → Server 실행 → 종료 신호 시 정리.

    python -m runiverse_ai_server.main      (저장소 루트에서. 설정은 환경 변수로만)

종료 코드: 0 정상 종료 · 1 워커 오류(재시작하면 커밋 위치부터 다시) · 2 설정 오류
"""

from __future__ import annotations

import signal
import sys
import time
from typing import Any, Callable

from .adapters.kafka import KafkaPublisher, KafkaRequestSource, consumer_config, producer_config
from .adapters.mysql import MySQLRepository
from .adapters.mysql import connect as mysql_connect
from .adapters.s3 import S3ObjectStore
from .adapters.sagemaker import SageMakerInvoker, sagemaker_runtime_config
from .completion_flow import CompletionFlow
from .config import ConfigError, Settings, load_settings
from .log import log_event
from .request_flow import RequestFlow
from .server import Server


def _confluent_consumer(config: dict) -> Any:
    from confluent_kafka import Consumer

    return Consumer(config)


def _confluent_producer(config: dict) -> Any:
    from confluent_kafka import Producer

    return Producer(config)


def _repository(db_connect: Callable[[Settings], Any], settings: Settings) -> MySQLRepository:
    return MySQLRepository(db_connect(settings), connect=lambda: db_connect(settings))


def build_server(settings: Settings, *, consumer_factory: Callable[[dict], Any] = _confluent_consumer,
                 producer_factory: Callable[[dict], Any] = _confluent_producer,
                 db_connect: Callable[[Settings], Any] = mysql_connect,
                 aws_client: Callable[..., Any] | None = None) -> Server:
    if aws_client is None:
        import boto3

        aws_client = boto3.Session(region_name=settings.aws_region).client

    publisher = KafkaPublisher(producer_factory(producer_config(settings)), settings.completion_topic)
    store = S3ObjectStore(aws_client("s3"), settings.ai_bucket_name)
    invoker = SageMakerInvoker(aws_client("sagemaker-runtime", config=sagemaker_runtime_config()),
                               settings.endpoint_name,
                               invocation_timeout_seconds=settings.invocation_timeout_seconds,
                               request_ttl_seconds=settings.request_ttl_seconds)

    request_flow = RequestFlow(store=store, invoker=invoker, publisher=publisher,
                               repo=_repository(db_connect, settings),  # 워커마다 DB 연결 1개
                               input_prefix=settings.input_prefix, endpoint_name=settings.endpoint_name)
    completion_flow = CompletionFlow(store=store, publisher=publisher, repo=_repository(db_connect, settings),
                                     response_deadline_seconds=settings.response_deadline_seconds)
    source = KafkaRequestSource(consumer_factory(consumer_config(settings)), settings.request_topic)
    return Server(source, request_flow, completion_flow, publisher,
                  completion_interval=settings.completion_poll_seconds)


def main() -> int:
    try:
        settings = load_settings()
    except ConfigError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    log_event("server", "starting", kafka_env=settings.kafka_env, request_topic=settings.request_topic,
              completion_topic=settings.completion_topic, group=settings.kafka_group_id,
              endpoint=settings.endpoint_name, bucket=settings.ai_bucket_name, input_prefix=settings.input_prefix,
              db=f"{settings.db_host}:{settings.db_port}/{settings.db_name}",
              completion_poll_seconds=settings.completion_poll_seconds,
              response_deadline_seconds=settings.response_deadline_seconds)
    server = build_server(settings)

    def on_signal(signum, _frame):
        log_event("server", "signal", signum=signum)
        server.stop()

    signal.signal(signal.SIGINT, on_signal)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, on_signal)

    server.start()
    while server.alive:  # 짧게 나눠 기다려야 Ctrl+C가 바로 처리된다
        time.sleep(0.5)
    return server.wait()


if __name__ == "__main__":
    sys.exit(main())
