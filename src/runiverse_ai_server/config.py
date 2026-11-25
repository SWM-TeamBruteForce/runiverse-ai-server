"""서버 설정. 환경 변수로만 받고(.env 파일 안 읽음), 잘못된 값이면 시작 시 ConfigError로 멈춘다."""

from typing import Literal, TypeVar

from pydantic import Field, FilePath, SecretStr, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

NonEmpty = Field(min_length=1)


class ConfigError(Exception):
    """설정 오류. 메시지에는 필드 이름과 사유만 담고 입력값(비밀번호 등)은 넣지 않는다."""


class DbSettings(BaseSettings):
    """AI DB 접속 값. 마이그레이션은 이것만 읽는다."""

    model_config = SettingsConfigDict(case_sensitive=False, validate_default=True, frozen=True)

    # AI DB (MySQL 8.4, 환경별 DB 분리)
    db_host: str = NonEmpty
    db_port: int = Field(default=3306, ge=1, le=65535)
    db_name: str = NonEmpty
    db_user: str = NonEmpty
    db_password: SecretStr


class KafkaSettings(BaseSettings):
    """Kafka 접속 값. Kafka 점검 스크립트는 이것만 읽는다."""

    model_config = SettingsConfigDict(case_sensitive=False, validate_default=True, frozen=True)

    # Kafka. 토픽·계정·그룹 이름이 이 값에서 만들어진다 (운영 prod: 2026-11-25 백엔드 Kafka 문서)
    kafka_env: Literal["dev", "test", "prod"]
    kafka_bootstrap_servers: str = NonEmpty
    kafka_password: SecretStr
    kafka_ca_path: FilePath = "config/local/kafka-ca.crt"  # 실행 디렉터리 기준

    @property
    def kafka_username(self) -> str:
        return f"{self.kafka_env}.runiverse-ai"

    @property
    def kafka_group_id(self) -> str:
        return f"{self.kafka_env}.runiverse-ai.record-card-music"

    @property
    def request_topic(self) -> str:
        return f"{self.kafka_env}.record-card.music.requested"

    @property
    def completion_topic(self) -> str:
        return f"{self.kafka_env}.record-card.music.generated"


class Settings(DbSettings, KafkaSettings):
    """서버 전체 설정 (DB·Kafka 포함)."""

    # AWS
    aws_region: str = "ap-northeast-2"
    ai_bucket_name: str = NonEmpty
    input_prefix: str = Field(pattern=r"^async/inputs/[^/]+$")  # 끝에 / 없음. 키는 {prefix}/{record_card_id}/...
    endpoint_name: str = NonEmpty

    # InvokeEndpointAsync 범위. 기본값은 SageMaker 기본값 (무응답 기준은 계획 9장 ③에서 확정)
    invocation_timeout_seconds: int = Field(default=900, ge=1, le=3600)
    request_ttl_seconds: int = Field(default=21600, ge=60, le=21600)

    # 완료 감지: 결과·실패 파일 확인 간격 (2026-11-16 SNS·SQS 대신 S3 확인)
    completion_poll_seconds: int = Field(default=10, ge=1, le=300)

    @property
    def response_deadline_seconds(self) -> int:
        """호출 후 이 시간까지 결과·실패 파일이 없으면 응답 없음 (SageMaker가 요청을 포기하는 시점)."""
        return self.invocation_timeout_seconds + self.request_ttl_seconds


S = TypeVar("S", bound=BaseSettings)


def load_settings(cls: type[S] = Settings) -> S:
    try:
        return cls()
    except ValidationError as exc:
        reasons = [
            f"{'.'.join(str(part) for part in err['loc'])}: {err['msg']}"
            for err in exc.errors(include_input=False, include_url=False)
        ]
        raise ConfigError("설정 오류 — " + "; ".join(reasons)) from None

