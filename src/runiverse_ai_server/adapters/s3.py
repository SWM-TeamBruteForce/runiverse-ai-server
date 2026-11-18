"""ObjectStore의 S3 구현. 입력 업로드와 결과/실패 파일 읽기."""

from __future__ import annotations

from typing import Any

from botocore.exceptions import BotoCoreError, ClientError

from ..ports import UploadError

_MISSING = {"NoSuchKey", "404", "NotFound"}


def parse_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"S3 URI가 아닙니다: {uri}")
    bucket, _, key = uri[len("s3://"):].partition("/")
    return bucket, key


class S3ObjectStore:
    def __init__(self, client: Any, bucket: str):
        self.client = client
        self.bucket = bucket

    def put(self, key: str, body: bytes) -> str:
        try:
            self.client.put_object(Bucket=self.bucket, Key=key, Body=body, ContentType="application/json")
        except ClientError as exc:
            error = exc.response.get("Error", {})
            raise UploadError(error.get("Code", "ClientError"), error.get("Message", str(exc))) from exc
        except BotoCoreError as exc:  # 네트워크 오류 등 응답 없음
            raise UploadError(type(exc).__name__, str(exc)) from exc
        return f"s3://{self.bucket}/{key}"

    def read_text(self, uri: str) -> str | None:
        """없으면 None. 그 밖의 오류는 그대로 올린다 (완료 흐름 실패 → 다음 확인 때 다시).

        ⚠️ 실행 주체에 s3:ListBucket이 없으면 없는 객체도 404가 아니라 403(AccessDenied)이 온다
        → "아직 결과 없음"을 구분하지 못한다. IAM에 해당 prefix ListBucket을 함께 준다.
        """
        bucket, key = parse_s3_uri(uri)
        try:
            body = self.client.get_object(Bucket=bucket, Key=key)["Body"].read()
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in _MISSING:
                return None
            raise
        return body.decode("utf-8", errors="replace")
