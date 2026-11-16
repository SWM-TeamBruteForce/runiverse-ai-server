-- AI 서버 요청 1건 = 행 1개. 설계: docs/AI_서버_아키텍처_설계.md 3장 (MySQL 8.4)
-- 시각은 UTC (연결 시 time_zone = '+00:00')
CREATE TABLE inference_request (
  inference_id        CHAR(36)      NOT NULL,       -- AI 서버가 호출 전에 발급 (UUID 문자열)
  user_id             BIGINT        NOT NULL,
  record_card_id      BIGINT        NOT NULL,
  status              VARCHAR(16)   NOT NULL,
  failed_stage        VARCHAR(16)   NULL,
  endpoint_name       VARCHAR(255)  NOT NULL,

  input_json          JSON          NOT NULL,       -- 요청 원본 (JSON 컬럼이라 키 순서·공백은 정리됨)
  result_json         JSON          NULL,           -- 컨테이너 성공/실패 파일 전체 (JSON일 때만)
  error_code          VARCHAR(64)   NULL,           -- 발행한 result.error.code
  error_message       MEDIUMTEXT    NULL,           -- 발행한 result.error.message (실패 파일 전체일 수 있음)

  input_s3_key        VARCHAR(1024) NOT NULL,
  output_location     VARCHAR(1024) NULL,           -- 202 응답 OutputLocation
  failure_location    VARCHAR(1024) NULL,           -- 202 응답 FailureLocation
  invoke_http_status  INT           NULL,
  invoke_error_code   VARCHAR(128)  NULL,
  invoke_message      TEXT          NULL,

  kafka_topic         VARCHAR(255)  NOT NULL,
  kafka_partition     INT           NOT NULL,
  kafka_offset        BIGINT        NOT NULL,

  created_at          DATETIME(6)   NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  invoked_at          DATETIME(6)   NULL,           -- INVOKING 기록 시각 (무응답 점검 기준)
  completed_at        DATETIME(6)   NULL,
  published_at        DATETIME(6)   NULL,           -- 완료 메시지 발행 확인 시각

  PRIMARY KEY (inference_id),
  UNIQUE KEY uq_req_kafka (kafka_topic, kafka_partition, kafka_offset),
  KEY ix_req_card (record_card_id, created_at),
  KEY ix_req_pending (status, invoked_at),           -- 무응답 점검 (MySQL은 부분 인덱스 없음)
  CONSTRAINT ck_req_status CHECK (status IN ('RECEIVED', 'UPLOADED', 'INVOKING', 'INVOKED', 'SUCCEEDED', 'FAILED')),
  CONSTRAINT ck_req_failed_stage CHECK (failed_stage IN ('INVOKE', 'INFERENCE'))
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;
