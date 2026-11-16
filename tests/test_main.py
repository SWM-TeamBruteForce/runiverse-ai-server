"""main: 설정 → 어댑터 조립 → Server. 외부 연결은 바꿔 끼워 실제 접속 없이 확인한다."""

import pytest

from runiverse_ai_server import main as main_module
from runiverse_ai_server.config import load_settings


@pytest.fixture
def env(monkeypatch, tmp_path):
    ca = tmp_path / "kafka-ca.crt"
    ca.write_text("dummy")
    for key, value in {"KAFKA_ENV": "dev", "KAFKA_BOOTSTRAP_SERVERS": "12.16.16.198:9092",
                       "KAFKA_PASSWORD": "kafka-secret", "KAFKA_CA_PATH": str(ca),
                       "AI_BUCKET_NAME": "bucket-test", "INPUT_PREFIX": "async/inputs/local",
                       "ENDPOINT_NAME": "endpoint-test", "DB_HOST": "localhost", "DB_NAME": "runiverse_ai_dev",
                       "DB_USER": "runiverse_ai", "DB_PASSWORD": "db-secret"}.items():
        monkeypatch.setenv(key, value)
    return monkeypatch


class Recorder:
    def __init__(self):
        self.consumers, self.producers, self.db_connections, self.clients = [], [], [], []

    def consumer(self, config):
        self.consumers.append(config)
        return type("C", (), {"subscribe": lambda self, topics: setattr(self, "topics", topics)})()

    def producer(self, config):
        self.producers.append(config)
        return object()

    def db_connect(self, settings):
        conn = object()
        self.db_connections.append(conn)
        return conn

    def client(self, service, config=None):
        self.clients.append((service, config))
        return object()


def test_build_server_wires_adapters(env):
    rec = Recorder()

    server = main_module.build_server(load_settings(), consumer_factory=rec.consumer,
                                      producer_factory=rec.producer, db_connect=rec.db_connect,
                                      aws_client=rec.client)

    [consumer_config] = rec.consumers
    assert consumer_config["group.id"] == "dev.runiverse-ai.record-card-music"
    assert consumer_config["enable.auto.commit"] is False
    assert len(rec.producers) == 1  # 두 워커가 같이 쓴다
    assert len(rec.db_connections) == 2  # 워커마다 DB 연결 1개
    assert server.request_flow.repo.conn is not server.completion_flow.repo.conn
    assert server.request_flow.publisher is server.completion_flow.publisher

    services = dict(rec.clients)
    assert set(services) == {"s3", "sagemaker-runtime"}
    assert services["sagemaker-runtime"].retries == {"max_attempts": 1, "mode": "standard"}

    assert server.request_flow.input_prefix == "async/inputs/local"
    assert server.request_flow.invoker.endpoint_name == "endpoint-test"
    assert server.request_flow.store.bucket == "bucket-test"
    assert server.completion_flow.response_deadline_seconds == 900 + 21600
    assert server.completion_interval == 10
    assert server.publisher.topic == "dev.record-card.music.generated"
    assert server.request_source.consumer.topics == ["dev.record-card.music.requested"]


def test_main_exits_2_on_bad_config(env, capsys):
    env.delenv("KAFKA_PASSWORD")

    assert main_module.main() == 2
    err = capsys.readouterr().err
    assert "kafka_password" in err
    assert "db-secret" not in err


def test_startup_log_has_no_secrets(env, capsys, monkeypatch):
    class FakeServer:
        def start(self):
            pass

        def stop(self):
            pass

        @property
        def alive(self):
            return False

        def wait(self, timeout=None):
            return 0

    monkeypatch.setattr(main_module, "build_server", lambda settings: FakeServer())

    assert main_module.main() == 0
    out = capsys.readouterr().out
    assert '"starting"' in out and "endpoint-test" in out
    assert "kafka-secret" not in out and "db-secret" not in out
