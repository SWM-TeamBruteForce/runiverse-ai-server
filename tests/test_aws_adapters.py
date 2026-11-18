"""S3·SageMaker 어댑터. botocore Stubber로 AWS 응답을 흉내 낸다 (네트워크 없음)."""

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import EndpointConnectionError
from botocore.stub import ANY, Stubber

from runiverse_ai_server.adapters.s3 import S3ObjectStore
from runiverse_ai_server.adapters.sagemaker import SageMakerInvoker, sagemaker_runtime_config
from runiverse_ai_server.ports import Invocation, InvokeError, UploadError

IID = "55555555-5555-4555-8555-555555555555"


def client(service):
    return boto3.client(service, region_name="ap-northeast-2", aws_access_key_id="x", aws_secret_access_key="x")


# ── S3 ──────────────────────────────────────────────


@pytest.fixture
def s3():
    c = client("s3")
    with Stubber(c) as stub:
        yield c, stub


def test_put_uploads_raw_bytes(s3):
    c, stub = s3
    body = '{"memo": "회고"}'.encode()
    stub.add_response("put_object", {}, {"Bucket": "bucket", "Key": "async/inputs/local/101/x.json", "Body": body,
                                         "ContentType": "application/json"})

    uri = S3ObjectStore(c, "bucket").put("async/inputs/local/101/x.json", body)

    assert uri == "s3://bucket/async/inputs/local/101/x.json"


def test_put_failure_is_upload_error(s3):
    c, stub = s3
    stub.add_client_error("put_object", service_error_code="AccessDenied", service_message="Access Denied",
                          http_status_code=403)

    with pytest.raises(UploadError) as exc:
        S3ObjectStore(c, "bucket").put("k", b"{}")

    assert (exc.value.error_code, exc.value.message) == ("AccessDenied", "Access Denied")


def test_put_network_error_is_upload_error():
    c = client("s3")
    c.put_object = lambda **_: (_ for _ in ()).throw(EndpointConnectionError(endpoint_url="https://s3"))

    with pytest.raises(UploadError) as exc:
        S3ObjectStore(c, "bucket").put("k", b"{}")

    assert exc.value.error_code == "EndpointConnectionError"


def test_read_text(s3):
    import io

    from botocore.response import StreamingBody

    c, stub = s3
    data = '{"error": {"message": "한글"}}'.encode()
    stub.add_response("get_object", {"Body": StreamingBody(io.BytesIO(data), len(data))},
                      {"Bucket": "out-bucket", "Key": "async/failures/test/x-error.out"})

    assert S3ObjectStore(c, "bucket").read_text("s3://out-bucket/async/failures/test/x-error.out") == data.decode()


def test_read_missing_returns_none(s3):
    c, stub = s3
    stub.add_client_error("get_object", service_error_code="NoSuchKey", http_status_code=404)

    assert S3ObjectStore(c, "bucket").read_text("s3://bucket/async/outputs/test/x.out") is None


def test_read_other_error_raises(s3):
    from botocore.exceptions import ClientError

    c, stub = s3
    stub.add_client_error("get_object", service_error_code="AccessDenied", http_status_code=403)

    with pytest.raises(ClientError):  # 완료 흐름 실패 → 다음 확인 때 다시 처리
        S3ObjectStore(c, "bucket").read_text("s3://bucket/k")


# ── SageMaker ─────────────────────────────────────────


@pytest.fixture
def sm():
    c = client("sagemaker-runtime")
    with Stubber(c) as stub:
        yield c, stub


def invoker(c):
    return SageMakerInvoker(c, "ep", invocation_timeout_seconds=900, request_ttl_seconds=21600)


def test_invoke_202_returns_locations(sm):
    c, stub = sm
    stub.add_response(
        "invoke_endpoint_async",
        {"InferenceId": IID, "OutputLocation": "s3://b/async/outputs/test/u.out",
         "FailureLocation": "s3://b/async/failures/test/u-error.out"},
        {"EndpointName": "ep", "InputLocation": "s3://b/in.json", "ContentType": "application/json",
         "InferenceId": IID, "InvocationTimeoutSeconds": 900, "RequestTTLSeconds": 21600},
    )

    assert invoker(c).invoke("s3://b/in.json", IID) == \
        Invocation("s3://b/async/outputs/test/u.out", "s3://b/async/failures/test/u-error.out")


def test_invoke_rejected_keeps_http_status(sm):
    c, stub = sm
    stub.add_client_error("invoke_endpoint_async", service_error_code="ValidationError",
                          service_message='Endpoint "ep" not found.', http_status_code=400,
                          expected_params={"EndpointName": "ep", "InputLocation": ANY, "ContentType": ANY,
                                           "InferenceId": ANY, "InvocationTimeoutSeconds": ANY,
                                           "RequestTTLSeconds": ANY})

    with pytest.raises(InvokeError) as exc:
        invoker(c).invoke("s3://b/in.json", IID)

    assert (exc.value.http_status, exc.value.error_code, exc.value.message) == \
        (400, "ValidationError", 'Endpoint "ep" not found.')


def test_invoke_no_response_has_no_http_status():
    c = client("sagemaker-runtime")
    c.invoke_endpoint_async = lambda **_: (_ for _ in ()).throw(EndpointConnectionError(endpoint_url="https://x"))

    with pytest.raises(InvokeError) as exc:
        invoker(c).invoke("s3://b/in.json", IID)

    assert (exc.value.http_status, exc.value.error_code) == (None, "EndpointConnectionError")


def test_runtime_client_retries_disabled():
    config = sagemaker_runtime_config()

    assert config.retries == {"max_attempts": 1, "mode": "standard"}
    assert isinstance(config, Config)
