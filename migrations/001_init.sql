-- AI 서버 요청 1건 = 행 1개. 설계: docs/AI_서버_아키텍처_설계.md 3장
CREATE TABLE inference_request (
  inference_id        uuid PRIMARY KEY,           -- AI 서버가 호출 전에 발급
  user_id             bigint      NOT NULL,
  record_card_id      bigint      NOT NULL,
  status              text        NOT NULL
    CHECK (status IN ('RECEIVED', 'UPLOADED', 'INVOKING', 'INVOKED', 'SUCCEEDED', 'FAILED')),
  failed_stage        text        CHECK (failed_stage IN ('INVOKE', 'INFERENCE')),
  endpoint_name       text        NOT NULL,

  input_json          jsonb       NOT NULL,       -- 요청 원본 (jsonb라 키 순서·공백은 정리됨)
  result_json         jsonb,                      -- 컨테이너 성공/실패 파일 전체 (JSON일 때만)
  error_code          text,                       -- 발행한 result.error.code
  error_message       text,                       -- 발행한 result.error.message

  input_s3_key        text        NOT NULL,
  output_location     text,                       -- 202 응답 OutputLocation
  failure_location    text,                       -- 202 응답 FailureLocation
  invoke_http_status  int,
  invoke_error_code   text,
  invoke_message      text,

  kafka_topic         text        NOT NULL,
  kafka_partition     int         NOT NULL,
  kafka_offset        bigint      NOT NULL,

  created_at          timestamptz NOT NULL DEFAULT now(),
  invoked_at          timestamptz,                -- INVOKING 기록 시각 (무응답 점검 기준)
  completed_at        timestamptz,
  published_at        timestamptz,                -- 완료 메시지 발행 확인 시각
  UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);

CREATE INDEX ix_req_card    ON inference_request (record_card_id, created_at DESC);
CREATE INDEX ix_req_pending ON inference_request (status, invoked_at) WHERE status IN ('INVOKING', 'INVOKED');
