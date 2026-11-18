"""Phase 4 검증: 실제 S3 업로드 + 실제 InvokeEndpointAsync → 없는 엔드포인트라 호출 거부 → INVOKE 실패 메시지.

- DB·Kafka 발행은 Fake (완료 메시지는 화면에만 출력)
- ⚠️ 과금 방지: 엔드포인트가 **존재하면 호출하지 않고 멈춘다** (0대여도 호출 시 인스턴스가 켜짐)
- 출력: 완료 메시지 (회고·캡션 없음)

    AI_BUCKET_NAME=... INPUT_PREFIX=async/inputs/local ENDPOINT_NAME=... \\
    .venv/Scripts/python scripts/invoke_reject_check.py --sample no_photo
"""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tests"))  # 점검용 Fake 재사용

SAMPLES_DIR = Path(__file__).parent / "samples"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="없는 엔드포인트 호출 거부 확인")
    parser.add_argument("--sample", default="no_photo")
    args = parser.parse_args(argv)

    import boto3
    from botocore.exceptions import ClientError

    from fakes import FakePublisher, FakeRepository
    from runiverse_ai_server.adapters.s3 import S3ObjectStore
    from runiverse_ai_server.adapters.sagemaker import SageMakerInvoker, sagemaker_runtime_config
    from runiverse_ai_server.ports import KafkaRecord
    from runiverse_ai_server.request_flow import RequestFlow

    env = {k: os.environ.get(k, "").strip() for k in ("AI_BUCKET_NAME", "INPUT_PREFIX", "ENDPOINT_NAME")}
    if not all(env.values()):
        raise SystemExit(f"환경 변수 필요: {', '.join(k for k, v in env.items() if not v)}")
    session = boto3.Session(region_name=os.environ.get("AWS_REGION", "ap-northeast-2"))

    try:
        status = session.client("sagemaker").describe_endpoint(EndpointName=env["ENDPOINT_NAME"])["EndpointStatus"]
        print(f"❌ 엔드포인트가 존재합니다 ({status}) → 호출하면 과금될 수 있어 중단")
        return 1
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "ValidationException":
            raise
    print(f"엔드포인트 {env['ENDPOINT_NAME']}: 없음 확인 → 호출 거부 기대")

    repo, publisher = FakeRepository(), FakePublisher()
    flow = RequestFlow(
        store=S3ObjectStore(session.client("s3"), env["AI_BUCKET_NAME"]),
        invoker=SageMakerInvoker(session.client("sagemaker-runtime", config=sagemaker_runtime_config()),
                                 env["ENDPOINT_NAME"], invocation_timeout_seconds=900, request_ttl_seconds=21600),
        publisher=publisher, repo=repo, input_prefix=env["INPUT_PREFIX"], endpoint_name=env["ENDPOINT_NAME"],
    )
    body = (SAMPLES_DIR / f"{args.sample}.json").read_bytes()
    with contextlib.redirect_stdout(io.StringIO()):  # 단계 로그(JSON)는 숨긴다
        flow.handle(KafkaRecord(topic="local-check", partition=0, offset=0, value=body))

    for row in repo.rows.values():  # 요청 메시지 오류면 DB 행이 없다
        print(f"업로드 s3://{env['AI_BUCKET_NAME']}/{row.input_s3_key}")
        print(f"DB(Fake) 상태 {row.status} · failed_stage {row.failed_stage} · "
              f"published_at {row.published_at is not None}")
    print("\n── 완료 메시지 (발행하지 않음) ──")
    for message in publisher.messages:
        print(json.dumps(message, indent=2, ensure_ascii=False))
    ok = len(publisher.messages) == 1 and publisher.messages[0]["failed_stage"] == "INVOKE"
    print("\n✅ 호출 거부 → INVOKE 실패 메시지" if ok else "\n❌ 기대와 다름")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
