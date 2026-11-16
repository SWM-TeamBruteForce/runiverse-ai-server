"""AsyncInvoker의 SageMaker 구현. 재시도하지 않는다 (CLAUDE.md 오류 원칙)."""

from __future__ import annotations

from typing import Any

from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from ..ports import Invocation, InvokeError


def sagemaker_runtime_config() -> Config:
    return Config(retries={"max_attempts": 1, "mode": "standard"})  # 시도 1번 = 재시도 없음


class SageMakerInvoker:
    def __init__(self, client: Any, endpoint_name: str, *, invocation_timeout_seconds: int,
                 request_ttl_seconds: int):
        self.client = client
        self.endpoint_name = endpoint_name
        self.invocation_timeout_seconds = invocation_timeout_seconds
        self.request_ttl_seconds = request_ttl_seconds

    def invoke(self, input_uri: str, inference_id: str) -> Invocation:
        try:
            response = self.client.invoke_endpoint_async(
                EndpointName=self.endpoint_name,
                InputLocation=input_uri,
                ContentType="application/json",
                InferenceId=inference_id,
                InvocationTimeoutSeconds=self.invocation_timeout_seconds,
                RequestTTLSeconds=self.request_ttl_seconds,
            )
        except ClientError as exc:  # 호출 거부 (400·500·503)
            error = exc.response.get("Error", {})
            raise InvokeError(exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode"),
                              error.get("Code", "ClientError"), error.get("Message", str(exc))) from exc
        except BotoCoreError as exc:  # 응답 없음 (네트워크·시간 초과) → http_status=null
            raise InvokeError(None, type(exc).__name__, str(exc)) from exc
        return Invocation(output_location=response["OutputLocation"],
                          failure_location=response.get("FailureLocation"))
