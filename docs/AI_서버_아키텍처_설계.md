# Runiverse AI 서버 — 아키텍처 설계

> `AI_서버_구현_계획.md`(범위·계약)를 바탕으로 한 **구조·DB·작업 순서** 설계.
> 작성 2026-11-13. 입출력 계약의 정본은 Notion「AI 기능 아키텍처 및 data 입출력」3·4장.

---

## 📌 0. 결정 사항 (2026-11-13)

| # | 항목 | 결정 |
|---|---|---|
| 1 | 기술 스택 | Python 3.11 · `confluent-kafka` · `boto3` · `pydantic-settings` · `PyMySQL` · `pytest` (DB 드라이버는 2026-11-16 `psycopg` 3 → `PyMySQL`) |
| 2 | 실행 구조 | 단일 프로세스, 워커 스레드 2개 (요청·완료) |
| 3 | 연결 정보 보관 | **AI 전용 DB (MySQL 8.4 컨테이너)**. test는 백엔드 test 서버 EC2에 AI 전용 컨테이너 (2026-11-16 PostgreSQL에서 전환) |
| 4 | `inference_id` | AI 서버가 발급하고 **S3 업로드 전에 DB에 기록** |
| 5 | 입력·결과 저장 | **원본 JSON 그대로** `JSON` 컬럼에 저장 (해시 안 씀) |
| 6 | 중복 소비 | `(topic, partition, offset)`으로 감지. 이미 처리한 메시지는 호출을 건너뜀 |
| 7 | 완료 감지 | ~~SNS → SQS + DLQ~~ → **202 응답의 결과 위치를 S3에서 주기적으로 확인** (2026-11-16). SNS·SQS 없음 |
| 8 | 완료 메시지 키 | `str(record_card_id)` (계획 9장 ⑤ 해소) |
| 9 | 업데이트 최적화 | 1차 구현에서는 **항상 전체 생성**. 저장만 해 두고 분기는 최적화 단계에서 |

---

## 🧱 1. 전체 구성

```txt
                  ┌───────────────────────── AI 서버 ─────────────────────────┐
[Kafka]           │ ① RequestWorker (스레드 A)                                │
requested ──────▶ │    파싱 → 중복 확인 → DB 기록 → S3 업로드 → 호출          │
                  │    ├ 거부 → Publisher (INVOKE 실패)                        │
                  │    └ 202 → 오프셋 커밋                                     │      [EC2]
                  │                                                           │ ◀──▶ MySQL
[S3 결과·실패 파일] ◀── SageMaker                                                   │
         ────────▶│ ② CompletionWorker (스레드 B)                             │
                  │    10초마다 발행 전 행 조회 → S3 파일 확인 → 저장 → 발행 │
generated ◀────── │ Publisher (Kafka Producer, 두 워커가 같이 씀)             │
                  └───────────────────────────────────────────────────────────┘
```

- 두 흐름은 독립된 루프. 만나는 곳은 DB `inference_request` 행 하나
- **워커 오류 처리** (2026-11-16)
  - 요청 워커: 처리 중 예외 → 커밋 없이 **서버 전체 중지**(종료 코드 1). 재시작하면 커밋 위치부터 다시 받음
  - 완료 워커: 1건 실패 → 로그 후 다음 건. `published_at`이 비어 있으니 다음 확인 때 다시 처리
  - 종료 신호 → 처리 중인 1건을 끝내고 Kafka 소비자 닫기·발행 대기분 flush
- 연결은 스레드끼리 나눠 쓰지 않는다: DB 연결은 워커마다 1개. Kafka Producer는 스레드 안전이라 공용
- 웹 프레임워크 없음. 헬스체크는 배포 위치가 정해지면 추가

---

## 📁 2. 저장소 구조

```txt
ai-server/
├ pyproject.toml
├ migrations/
│  └ 001_init.sql
├ src/runiverse_ai_server/
│  ├ config.py          # Settings + 시작 시 검증
│  ├ log.py             # JSON 한 줄 로그 (inference_id·record_card_id·stage)
│  ├ messages.py        # 순수 함수: 요청 파싱, S3 키, 완료 메시지 조립(4가지)
│  ├ ports.py           # Protocol: ObjectStore·AsyncInvoker·Publisher·InferenceRepository
│  ├ adapters/          # kafka.py · s3.py · sagemaker.py · mysql.py
│  ├ request_flow.py    # 흐름 ①
│  ├ completion_flow.py # 흐름 ②
│  ├ workers.py         # 워커 루프 (요청: poll→처리→커밋 / 완료: 수신→처리)
│  ├ server.py          # 워커 스레드 2개 실행·중지·정리
│  ├ migrate.py         # migrations/ 적용
│  └ main.py            # 설정 → 어댑터 조립 → Server 실행 (Phase 4·5 어댑터 후)
└ tests/
   ├ fakes.py           # 포트별 메모리 Fake
   ├ test_messages.py
   ├ test_request_flow.py
   └ test_completion_flow.py
```

- 추상화는 `ports.py`만 둔다. Fake 구현(CLAUDE.md 규칙)에 필요한 만큼
- 흐름 로직은 포트만 의존 → AWS·Kafka·DB 없이 테스트

---

## 🗄️ 3. DB 설계

### 3.1 테이블

```sql
-- AI 서버 요청 1건 = 행 1개. 설계: docs/AI_서버_아키텍처_설계.md 3장 (MySQL 8.4)
-- 시각은 UTC (연결 시 time_zone = '+00:00')
CREATE TABLE inference_request (
  inference_id        CHAR(36)      NOT NULL,       -- AI 서버가 호출 전에 발급 (UUID 문자열)
  user_id             CHAR(36)      NOT NULL,       -- 백엔드 UUID 문자열 (2026-11-17, 002_uuid_ids.sql)
  record_card_id      CHAR(36)      NOT NULL,
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
```

- 정본은 `migrations/001_init.sql` (MySQL 8.4, 2026-11-16 PostgreSQL에서 전환)
- MySQL 차이: 부분 인덱스가 없어 `ix_req_pending`은 일반 인덱스, `inference_id`는 `CHAR(36)`, 시각은 UTC `DATETIME(6)`
- ⚠️ MySQL은 DDL을 되돌리지 못한다 → 마이그레이션 중간 실패 시 앞 문장이 남음. 파일 하나에 DDL은 되도록 하나씩

- 사진 정보는 `input_json.photos`, 음악 키는 `result_json.photo_music`에 있다 → 사진 테이블 없음. 사진별 조회가 잦아지면 추가
- 환경별로 **DB를 따로** 둔다: `runiverse_ai_dev` · `runiverse_ai_test` · `runiverse_ai_prod`
- 마이그레이션은 `migrations/NNN_*.sql` 순번 파일. ORM 없이 SQL 직접 작성

### 3.2 상태 전이

```txt
RECEIVED ──업로드──▶ UPLOADED ──호출 직전──▶ INVOKING ──202──▶ INVOKED ──결과 파일──▶ SUCCEEDED
   │                                            │                    └─실패 파일·응답 없음──▶ FAILED (INFERENCE)
   └─업로드 실패──▶ FAILED (INVOKE)              └─거부·응답 없음──▶ FAILED (INVOKE)
발행 확인 시 published_at 기록 (SUCCEEDED·FAILED 공통)
```

- **`INVOKING`** (2026-11-16 결정): 호출 직후·202 기록 전에 서버가 죽어도 **다시 호출하지 않기** 위해 호출 직전에 남긴다 → 호출은 많아야 1번
  - 대가: 호출 **전**에 죽으면 호출되지 않은 채 남는다 → 무응답 점검(4.3)이 `INFERENCE_FAILED`로 발행, 사용자 재요청
  - SageMaker `InferenceId`로 중복 호출을 막는 기능은 문서에서 확인되지 않아 쓰지 않는다
- `mark_invoked`는 `INVOKING`일 때만 바꾼다 (완료가 먼저 기록됐으면 되돌리지 않음)

### 3.3 저장 원칙

- 입력·결과 JSON은 **원본 그대로** 저장. 해석·가공하지 않음
- DB의 `result_json`은 컨테이너 결과 전체, Kafka 완료 메시지 `result`는 Notion 4.1 필드만 추출 — **둘은 형식이 다르다**
- 원본 보관 위치는 **이 DB 하나**. 로그·로컬 파일에는 남기지 않음
- 보관 기간: 미정. 업데이트 비교에는 카드별 직전 성공 1건만 필요 → 정리 기준(예: 90일)은 나중에 결정

---

## 🔀 4. 처리 흐름

### 4.1 요청 (Kafka → SageMaker)

| 단계 | 처리 | 실패·예외 |
|---|---|---|
| 파싱 | JSON 파싱, `user_id`·`record_card_id` 존재 확인 | 오류 → 호출하지 않고 `INVOKE` 실패 발행(`INVALID_MESSAGE`) → 커밋. DB 기록 없음 (5장) |
| 중복 확인 | `(topic, partition, offset)` 조회 | 아래 표 |
| DB 기록 | `inference_id` 발급, `RECEIVED` INSERT | |
| 업로드 | **원본 바이트 그대로** `{INPUT_PREFIX}/{record_card_id}/{inference_id}.json` → `UPLOADED` | ⚠️ 5장 |
| 호출 | `INVOKING`·`invoked_at` 기록 → `InvokeEndpointAsync(InputLocation, InferenceId, ContentType=application/json)`. boto3 재시도 끔(`max_attempts=1`) | `ClientError` → `FAILED(INVOKE)` 저장 → 발행 → `published_at` → 커밋 |
| 202 | `INVOKED`, 응답 헤더의 `output_location`·`failure_location` 저장 → 오프셋 커밋 | |

**같은 offset을 다시 받았을 때**

| 기존 행 상태 | 처리 |
|---|---|
| 없음 | 새 요청으로 처리 |
| `RECEIVED`·`UPLOADED` (호출 전에 중단) | **같은 `inference_id`로** 업로드부터 이어서 처리 |
| `INVOKING` (호출됐는지 모름) | **다시 호출하지 않고** 커밋. 완료 워커가 응답 없음(4.3)으로 마무리 |
| `INVOKED`·`SUCCEEDED` | 호출하지 않고 커밋 |
| `FAILED(INVOKE)`이고 `published_at` 없음 | 완료 메시지만 다시 발행 → 커밋 |
| `FAILED`이고 `published_at` 있음 | 커밋 |

- 사용자 재요청은 offset이 다르므로 새 요청으로 처리됨 (요청건대로 처리 원칙 유지)
- 오토 커밋은 끄고 수동 커밋만 사용

### 4.2 완료 (S3 확인 → Kafka) — 2026-11-16 SNS·SQS 대신

| 단계 | 처리 |
|---|---|
| 조회 | `COMPLETION_POLL_SECONDS`(기본 10초)마다 **발행 전** 행 조회: `INVOKING`·`INVOKED`, 그리고 저장 후 발행 못 한 `SUCCEEDED`·`FAILED(INFERENCE)` (최대 100건, 오래된 호출부터) |
| 확인 | `output_location` → 없으면 `failure_location`을 S3에서 읽기 (없으면 404 → 아직) |
| 판정 | 결과 파일 → 성공 / 실패 파일 → 실패 / 둘 다 없음 → 기다림 / 제한 시간 넘음 → `INFERENCE_FAILED` (응답 없음) |
| 조립 | Notion 4.1 형식으로 추출 (아래 표) |
| 저장 | `result_json`·`status`·`error_code`·`error_message`·`completed_at` |
| 발행 | 메시지 키 `record_card_id`. 발행 확인 → `published_at` |

- 제한 시간 = `INVOCATION_TIMEOUT_SECONDS` + `REQUEST_TTL_SECONDS` (기본 900 + 21600초). SageMaker가 요청을 포기하는 시점
- 발행 실패 → `published_at`이 비어 있으니 다음 확인 때 다시 발행 (추론 재실행 아님)
- 결과 파일 이름은 SageMaker UUID → 202 응답의 위치를 DB에 저장해 두고 그대로 읽는다
- ⚠️ IAM에 `s3:ListBucket`(해당 prefix)이 없으면 없는 파일도 403 → "아직"을 구분 못 함

**완료 메시지 `result` 조립 (Notion 4.1)**

| 경우 | `result` |
|---|---|
| 성공 | `base_music`의 `s3_key`·`duration_seconds`, `photo_music[]`의 `record_card_image_id`·`s3_key`·`duration_seconds`만 |
| 실패 파일이 JSON | `{error: {code, message}}`만 (`detail` 제외) |
| 실패 파일이 JSON 아님 | `{error: {code: INFERENCE_FAILED, message: 파일 내용 전체}}` |
| 실패 파일 없음 | `{error: {code: INFERENCE_FAILED, message: ...}}` |

- 완료 메시지 최상위 필드: `inference_id`·`record_card_id`·`status`·`failed_stage`·`invoke`·`result`. **`user_id`는 넣지 않음**
- 값은 바꾸거나 새로 계산하지 않는다 (`duration_seconds` 반올림 등 금지)

### 4.3 무응답 (계획 9장 ③)

- 호출할 때 `InvocationTimeoutSeconds`·`RequestTTLSeconds`를 명시
- 완료 워커가 **제한 시간까지 결과·실패 파일이 없으면** `INFERENCE_FAILED`(응답 없음) 발행 (2026-11-16)
- **`INVOKING`에 멈춘 행**(호출 전후에 서버 중단)은 결과 위치를 모른다 → 제한 시간 후 같은 방식으로 발행
- 시간 초과 시 SageMaker가 실패 파일을 남기는지는 dev 검증 ④에서 확인 (남기면 그 내용으로 발행)
- 기준 시간(사용자에게 약속할 대기 시간)은 백엔드와 협의 (계획 9장 ③)

### 4.4 업데이트 최적화 (나중)

- 새 요청과 `record_card_id`의 직전 `SUCCEEDED` 행 `input_json`을 필드별로 비교해 분기

```python
prev = repo.latest_succeeded(record_card_id)
if prev is not None:
    old, new = prev.input_json, request
    memo_changed = old.get("memo") != new.get("memo")
    old_photos = {p["record_card_image_id"]: p for p in old["photos"]}
    changed_photos = [p for p in new["photos"] if old_photos.get(p["record_card_image_id"]) != p]
```

- 컨테이너 쪽 제약 (`../ai/src/runiverse_music/pipeline.py`)
  - memo 분석 결과 `emotion_direction`이 **모든 곡의 조성**을 정함 → memo가 바뀌면 전체가 바뀔 수 있음
  - seed가 사진 **배열 순서** 기준 → 순서만 바뀌어도 결과가 달라짐
  - 컨테이너는 일부 사진만 생성하는 입력을 받지 않음
- → 최적화 단계에서 `../ai` 입력·결과 계약을 함께 바꿔야 함

---

## ⚠️ 5. 계약에 없는 경우

| 경우 | 처리 | 상태 |
|---|---|---|
| S3 업로드 실패 (호출 전) | `failed_stage=INVOKE`, `invoke.http_status=null`, 원인은 `invoke.error_code`·`message` | ✅ 결정 (2026-11-14, Notion 반영) |
| HTTP 응답이 없는 호출 실패 (네트워크 등) | `failed_stage=INVOKE`, `invoke.http_status=null`, 원인은 `invoke.error_code`·`message` | ✅ 결정 (2026-11-14, Notion 반영) |
| 요청 메시지 오류 (JSON 아님, `user_id`·`record_card_id` 없음) | 호출하지 않고 **완료 토픽에 실패 발행**: `failed_stage=INVOKE`, `invoke.http_status=null`, `invoke.error_code=INVALID_MESSAGE`, `inference_id=null`, `result=null`. `record_card_id`는 본문에서 읽을 수 있으면 담고, 없으면 `null` (Kafka 키는 쓰지 않음). `message`에 본문 원문 금지. DB에 기록하지 않음 | ✅ 결정 (2026-11-14, Notion 반영) |
| 완료 메시지 파티션 키 | 메시지 키 = `str(record_card_id)`. 같은 카드의 완료 메시지는 **AI가 발행한 순서대로** 전달됨 (사용자 요청 순서와는 다를 수 있음) | ✅ 결정 (2026-11-13) |

- 위 결정은 모두 Notion「AI 기능 아키텍처 및 data 입출력」3장·4.1·4.3에 반영함 (2026-11-14)
- `record_card_id`가 `null`인 완료 메시지는 Kafka 키 없이 발행

---

## ⚙️ 6. 설정 추가

| 값 | 설명 |
|---|---|
| `DB_HOST`·`DB_PORT`·`DB_NAME`·`DB_USER` | 환경별 DB |
| `DB_PASSWORD` | **환경 변수로만** |
| `COMPLETION_POLL_SECONDS` | 결과 파일 확인 간격 (기본 10초, 1~300) |
| `INVOCATION_TIMEOUT_SECONDS`·`REQUEST_TTL_SECONDS` | 4.3 |

- 기존 값(`KAFKA_ENV`·`AI_BUCKET_NAME`·`INPUT_PREFIX`·`ENDPOINT_NAME` 등)은 계획 문서 5장
- 잘못된 값이면 시작 시 실패

---

## 🛡️ 7. DB 서버

| 항목 | 기준 |
|---|---|
| 위치 (test) | 백엔드 test 서버 EC2(`ec2-private-test-application-infra-runiverse-dev`)에 **AI 전용 `mysql:8.4` 컨테이너** (2026-11-16) |
| 네트워크 | private 서브넷, 퍼블릭 IP 없음. 3306은 같은 서버의 AI 서버에서만 (외부 공개 안 함) |
| 디스크 | EBS 암호화, 스냅샷 백업 |
| 계정 | 앱 전용 사용자. 비밀번호는 환경 변수로만 |
| 운영 | 패치·백업·모니터링은 직접 (RDS가 아님) |

- 생성·설정은 **Console 단계별 안내**로 진행 (과금 시작 지점은 미리 알림)

---

## 🗺️ 8. 작업 계획

앞 단계 검증을 통과하기 전에 다음으로 가지 않는다.

| Phase | 작업 | 검증 | Console | 과금 |
|---|---|---|---|---|
| 0. 준비 | 저장소, `pyproject`, 설정·로그 골격 | 잘못된 설정이면 시작 실패 | — | — |
| 1. 순수 로직 | `messages.py` | **검증 ①** 단위 테스트 (완료 메시지 4가지) | — | — |
| 2. 흐름 + Fake | `ports`·`fakes`·두 흐름 | Fake로 성공·컨테이너 실패·무응답·거부·중복 offset | — | — |
| 2.5 DB | 마이그레이션, MySQL Repository | 로컬 Docker `mysql:8.4`: 상태 전이, 중복 offset, `published_at` 멱등 | — | — |
| 3. Kafka | Kafka 어댑터, 수동 커밋, `main` | **검증 ②** `KAFKA_ENV=dev`: 소비·발행, 재시작 후 재소비 | — | — |
| 4. S3 + 호출 | S3·SageMaker 어댑터 | 없는 엔드포인트로 호출 거부 → `INVOKE` 발행 | IAM 권한 | — |
| 4.5 DB 서버 | test 서버에 MySQL 컨테이너 | 연결·마이그레이션 적용 | — (기존 EC2) | — |
| ~~5. 알림 인프라~~ | 없음 (2026-11-16 S3 확인으로 대체) | — | — | — |
| 6. 스모크 | `main` 조립, 실제 결과·실패 파일로 완료 발행 | 컨테이너 단독 스모크 → **검증 ③** | 엔드포인트 생성 | **시작·지속** |
| 7. 실패 경로 | 타임아웃 파라미터, 4.3 결정 | **검증 ④** 400·거부·실패 파일·시간 초과 | 끝나면 엔드포인트 정리 | — |
| 8. 통합 | Dockerfile, 문서 갱신 | 백엔드 통합 테스트 | 배포 위치 | — |

- 완료 감지는 S3 파일 확인이라 SNS 알림 형식에 의존하지 않는다

---

## 📝 9. CLAUDE.md 수정안 (✅ 2026-11-14 반영)

| 위치 | 현재 | 수정안 |
|---|---|---|
| 2장 ① 중복 | 요청 ID가 없어 **중복 판별은 하지 않는다** | Kafka `(topic, partition, offset)`으로 같은 메시지 재수신만 감지한다. 재요청은 offset이 달라 요청건대로 처리 |
| 1장 AI-SRV-07 | 보관 방식 미정 | AI 전용 DB(EC2 PostgreSQL) `inference_request` |
| 3장 저장소 구조 | 미정 | Python 3.11 · confluent-kafka · boto3 · psycopg 3. 이 문서 2장 |
| Don't | 회고 원문·사진 캡션·입력 JSON 본문을 로그·**로컬 저장소**에 남기기 | 로그·**로컬 파일**에 남기기. 원본 보관은 AI DB `input_json`에만 |
| Docs 표 | — | `docs/AI_서버_아키텍처_설계.md` 추가 |
