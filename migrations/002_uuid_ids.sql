-- 백엔드 ID가 UUID 문자열 (2026-11-17, test 토픽 실제 메시지 확인). BIGINT → CHAR(36)
-- 값은 백엔드가 보낸 그대로 저장한다 (대소문자 정리 없음). 인덱스 ix_req_card는 함께 다시 만들어진다
ALTER TABLE inference_request
  MODIFY user_id        CHAR(36) NOT NULL,
  MODIFY record_card_id CHAR(36) NOT NULL;
