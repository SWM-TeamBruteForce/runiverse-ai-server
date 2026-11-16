"""SageMaker Async 스모크: 샘플 JSON → S3 업로드 → InvokeEndpointAsync → 결과 파일 원문 출력.

사용 (ai-server 루트에서):
    AWS_PROFILE=<프로필> AI_BUCKET_NAME=... INPUT_PREFIX=async/inputs/local ENDPOINT_NAME=... \\
    .venv/Scripts/python scripts/smoke_invoke.py --sample no_photo

- 샘플: scripts/samples/{no_photo, with_photo, invalid_unknown_field}.json
- SNS 미구성 상태라 S3 결과 위치를 주기적으로 확인한다
- 판정하지 않고 받은 그대로 출력한다 (실패 파일 형식 확인용)
- ⚠️ 엔드포인트가 0대에서 시작하면 인스턴스가 뜨면서 과금되고, 콜드 스타트로 10분 이상 걸릴 수 있다
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from pathlib import Path

SAMPLES_DIR = Path(__file__).parent / "samples"


def input_key(input_prefix: str, record_card_id: int, inference_id: str) -> str:
    return f"{input_prefix}/{record_card_id}/{inference_id}.json"


def parse_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"S3 URI가 아닙니다: {uri}")
    bucket, _, key = uri[len("s3://"):].partition("/")
    return bucket, key


def load_sample(name: str) -> tuple[bytes, int]:
    """샘플 원본 바이트(수정 없이 업로드)와 S3 경로용 record_card_id."""
    body = (SAMPLES_DIR / f"{name}.json").read_bytes()
    record_card_id = json.loads(body).get("record_card_id")
    if not isinstance(record_card_id, int):
        raise ValueError(f"{name}.json에 정수 record_card_id가 없습니다")
    return body, record_card_id


def _exists(s3, uri: str) -> bool:
    from botocore.exceptions import ClientError

    bucket, key = parse_s3_uri(uri)
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def wait_for_result(s3, output_uri: str, failure_uri: str | None, timeout: float,
                    interval: float = 15.0, sleep=time.sleep, clock=time.monotonic) -> tuple[str, str]:
    """성공·실패 위치 중 먼저 생긴 파일의 원문. 없으면 TimeoutError."""
    deadline = clock() + timeout
    while True:
        for status, uri in (("success", output_uri), ("failure", failure_uri)):
            if uri and _exists(s3, uri):
                bucket, key = parse_s3_uri(uri)
                return status, s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8", errors="replace")
        if clock() >= deadline:
            raise TimeoutError(f"{timeout:.0f}초 안에 결과가 없습니다: {output_uri}")
        sleep(interval)


def _required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit(f"환경 변수 {name}가 필요합니다")
    return value


def _print_body(title: str, body: str) -> None:
    print(f"\n── {title} (원문) ──")
    try:
        print(json.dumps(json.loads(body), indent=2, ensure_ascii=False))
    except json.JSONDecodeError:
        print(body)
        print("⚠️ JSON이 아닙니다")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="SageMaker Async 스모크")
    parser.add_argument("--sample", default="no_photo", help="scripts/samples/<이름>.json")
    parser.add_argument("--timeout", type=float, default=1800, help="결과 대기 상한(초)")
    parser.add_argument("--interval", type=float, default=15, help="S3 확인 간격(초)")
    parser.add_argument("--invocation-timeout", type=int, default=900, help="InvocationTimeoutSeconds")
    args = parser.parse_args(argv)

    bucket = _required_env("AI_BUCKET_NAME")
    prefix = _required_env("INPUT_PREFIX").rstrip("/")
    endpoint = _required_env("ENDPOINT_NAME")
    region = os.environ.get("AWS_REGION", "ap-northeast-2")

    import boto3
    from botocore.config import Config
    from botocore.exceptions import BotoCoreError, ClientError

    session = boto3.Session(region_name=region)
    s3 = session.client("s3")
    runtime = session.client("sagemaker-runtime", config=Config(retries={"max_attempts": 1, "mode": "standard"}))

    body, record_card_id = load_sample(args.sample)
    inference_id = str(uuid.uuid4())
    key = input_key(prefix, record_card_id, inference_id)
    input_uri = f"s3://{bucket}/{key}"

    print(f"리전 {region} · 엔드포인트 {endpoint} · 샘플 {args.sample}")
    print(f"inference_id {inference_id}")
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")
    print(f"업로드 {input_uri}")

    started = time.monotonic()
    try:
        invoked = runtime.invoke_endpoint_async(
            EndpointName=endpoint,
            InputLocation=input_uri,
            ContentType="application/json",
            InferenceId=inference_id,
            InvocationTimeoutSeconds=args.invocation_timeout,
        )
    except ClientError as exc:  # 호출 단계 실패 → 완료 메시지 failed_stage=INVOKE
        print("\n❌ 호출 거부")
        print(f"http_status {exc.response.get('ResponseMetadata', {}).get('HTTPStatusCode')}")
        print(f"error_code  {exc.response.get('Error', {}).get('Code')}")
        print(f"message     {exc.response.get('Error', {}).get('Message')}")
        return 1
    except BotoCoreError as exc:  # SageMaker 응답 없음 → invoke.http_status=null
        print(f"\n❌ 응답 없음 ({type(exc).__name__}): {exc}")
        return 1

    print("\n── 호출 응답 ──")
    print(f"http_status      {invoked['ResponseMetadata']['HTTPStatusCode']}")
    print(f"InferenceId      {invoked.get('InferenceId')}")
    print(f"OutputLocation   {invoked.get('OutputLocation')}")
    print(f"FailureLocation  {invoked.get('FailureLocation')}")
    print(f"\n결과 대기 (최대 {args.timeout:.0f}초, {args.interval:.0f}초 간격)...", flush=True)

    try:
        status, result = wait_for_result(s3, invoked["OutputLocation"], invoked.get("FailureLocation"),
                                         args.timeout, args.interval)
    except TimeoutError as exc:
        print(f"\n⏱️ {exc}")
        return 3

    elapsed = time.monotonic() - started
    _print_body("성공 결과 OutputLocation" if status == "success" else "실패 결과 FailureLocation", result)
    print(f"\n{'✅ 성공' if status == 'success' else '❌ 실패'} · 호출 후 {elapsed:.0f}초")
    return 0 if status == "success" else 1


if __name__ == "__main__":
    sys.exit(main())
