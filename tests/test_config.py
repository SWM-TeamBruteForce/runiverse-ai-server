"""설정은 환경 변수로만 받고, 잘못되면 시작 시 실패한다."""

import pytest

from runiverse_ai_server.config import ConfigError, DbSettings, KafkaSettings, load_settings


@pytest.fixture
def env(monkeypatch, tmp_path):
    """유효한 최소 환경. 각 테스트가 값 하나씩 바꿔 실패를 확인한다."""
    ca = tmp_path / "kafka-ca.crt"
    ca.write_text("dummy")
    values = {
        "KAFKA_ENV": "dev",
        "KAFKA_BOOTSTRAP_SERVERS": "kafka.example:9092",
        "KAFKA_PASSWORD": "kafka-secret",
        "KAFKA_CA_PATH": str(ca),
        "AI_BUCKET_NAME": "bucket-test",
        "INPUT_PREFIX": "async/inputs/local",
        "ENDPOINT_NAME": "endpoint-test",
        "SQS_QUEUE_URL": "https://sqs.ap-northeast-2.amazonaws.com/000000000000/queue-test",
        "DB_HOST": "localhost",
        "DB_NAME": "runiverse_ai_dev",
        "DB_USER": "runiverse_ai",
        "DB_PASSWORD": "db-secret",
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value)
    return monkeypatch


def test_loads_valid_environment(env):
    settings = load_settings()

    assert settings.kafka_env == "dev"
    assert settings.endpoint_name == "endpoint-test"
    assert settings.db_port == 5432
    assert settings.aws_region == "ap-northeast-2"


def test_derives_kafka_names_from_env(env):
    settings = load_settings()

    assert settings.kafka_username == "dev.runiverse-ai"
    assert settings.kafka_group_id == "dev.runiverse-ai.record-card-music"
    assert settings.request_topic == "dev.record-card.music.requested"
    assert settings.completion_topic == "dev.record-card.music.generated"


def test_uses_sagemaker_default_timeouts(env):
    settings = load_settings()

    assert settings.invocation_timeout_seconds == 900
    assert settings.request_ttl_seconds == 21600


@pytest.mark.parametrize("missing", ["KAFKA_PASSWORD", "DB_PASSWORD", "ENDPOINT_NAME", "SQS_QUEUE_URL"])
def test_fails_when_required_value_missing(env, missing):
    env.delenv(missing)

    with pytest.raises(ConfigError, match=missing.lower()):
        load_settings()


@pytest.mark.parametrize("value", ["test", "prod"])  # dev만 사용 (2026-11-16)
def test_accepts_only_dev_kafka_env(env, value):
    env.setenv("KAFKA_ENV", value)

    with pytest.raises(ConfigError, match="kafka_env"):
        load_settings()


@pytest.mark.parametrize("prefix", ["async/outputs/local", "async/inputs/local/", "/async/inputs/local"])
def test_rejects_bad_input_prefix(env, prefix):
    env.setenv("INPUT_PREFIX", prefix)

    with pytest.raises(ConfigError, match="input_prefix"):
        load_settings()


def test_rejects_missing_ca_file(env, tmp_path):
    env.setenv("KAFKA_CA_PATH", str(tmp_path / "none.crt"))

    with pytest.raises(ConfigError, match="kafka_ca_path"):
        load_settings()


@pytest.mark.parametrize(
    ("key", "value"),
    [("INVOCATION_TIMEOUT_SECONDS", "0"), ("INVOCATION_TIMEOUT_SECONDS", "3601"),
     ("REQUEST_TTL_SECONDS", "59"), ("REQUEST_TTL_SECONDS", "21601")],
)
def test_rejects_timeout_out_of_sagemaker_range(env, key, value):
    env.setenv(key, value)

    with pytest.raises(ConfigError, match=key.lower()):
        load_settings()


def test_error_message_does_not_leak_secrets(env):
    env.setenv("KAFKA_ENV", "prod")  # 다른 값이 틀려도 비밀번호는 메시지에 없어야 한다

    with pytest.raises(ConfigError) as exc:
        load_settings()

    assert "kafka-secret" not in str(exc.value)
    assert "db-secret" not in str(exc.value)


def test_secrets_hidden_in_repr(env):
    settings = load_settings()

    assert "kafka-secret" not in repr(settings)
    assert settings.kafka_password.get_secret_value() == "kafka-secret"


def test_db_settings_load_without_kafka_or_aws(env):
    for key in ["KAFKA_ENV", "KAFKA_PASSWORD", "ENDPOINT_NAME", "SQS_QUEUE_URL"]:
        env.delenv(key)

    settings = load_settings(DbSettings)  # 마이그레이션은 DB 값만 있으면 된다

    assert (settings.db_host, settings.db_port, settings.db_name) == ("localhost", 5432, "runiverse_ai_dev")


def test_db_settings_require_password(env):
    env.delenv("DB_PASSWORD")

    with pytest.raises(ConfigError, match="db_password"):
        load_settings(DbSettings)


def test_kafka_settings_load_without_aws_or_db(env):
    for key in ["ENDPOINT_NAME", "SQS_QUEUE_URL", "DB_HOST", "DB_PASSWORD"]:
        env.delenv(key)

    settings = load_settings(KafkaSettings)  # Kafka 점검 스크립트는 Kafka 값만 있으면 된다

    assert (settings.kafka_username, settings.request_topic) == \
        ("dev.runiverse-ai", "dev.record-card.music.requested")
