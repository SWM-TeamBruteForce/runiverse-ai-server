# Runiverse AI 서버 — 아키텍처 설계

> `AI_서버_구현_계획.md`(범위·계약)를 바탕으로 한 **구조·DB·작업 순서** 설계.
> 작성 2026-11-13. 입출력 계약의 정본은 Notion「AI 기능 아키텍처 및 data 입출력」3·4장.

---

## 📌 0. 결정 사항 (2026-11-13)

| # | 항목 | 결정 |
|---|---|---|
| 1 | 기술 스택 | Python 3.11 · `confluent-kafka` · `boto3` · `pydantic-settings` · `psycopg` 3 · `pytest` |
| 2 | 실행 구조 | 단일 프로세스, 워커 스레드 2개 (요청·완료) |
| 3 | 연결 정보 보관 | **AI 전용 DB (EC2 + PostgreSQL)**. 계획 문서 9장 ② 해소 |
| 4 | `inference_id` | AI 서버가 발급하고 **S3 업로드 전에 DB에 기록** |
| 5 | 입력·결과 저장 | **원본 JSON 그대로** `jsonb`에 저장 (해시 안 씀) |
| 6 | 중복 소비 | `(topic, partition, offset)`으로 감지. 이미 처리한 메시지는 호출을 건너뜀 |
| 7 | SNS 수신 | SNS → SQS 구독 + DLQ. AI 서버가 운영 (9장 ④) |
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
                  │                                                           │ ◀──▶ PostgreSQL
[SQS] ◀─ SNS ◀── SageMaker                                                    │
         ────────▶│ ② CompletionWorker (스레드 B)                             │
                  │    알림 → DB 조회 → 결과/실패 JSON 읽기 → 저장 → 발행    │
generated ◀────── │ Publisher (Kafka Producer, 두 워커가 같이 씀)             │
                  └───────────────────────────────────────────────────────────┘
```

- 두 흐름은 독립된 루프. 만나는 곳은 DB `inference_request` 행 하나
- **워커 오류 처리** (2026-11-16)
  - 요청 워커: 처리 중 예외 → 커밋 없이 **서버 전체 중지**(종료 코드 1). 재시작하면 커밋 위치부터 다시 받음
  - 완료 워커: 알림 1건 실패 → 로그 후 다음 알림. SQS 메시지를 지우지 않았으니 다시 오고, 한도를 넘으면 DLQ
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
│  ├ notification.py    # SNS 알림 파싱 → dev 스모크 후 확정
│  ├ ports.py           # Protocol: ObjectStore·AsyncInvoker·NotificationQueue·Publisher·InferenceRepository
│  ├ adapters/          # kafka.py · s3.py · sagemaker.py · sqs.py · postgres.py
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
CREATE TABLE inference_request (
  inference_id        uuid PRIMARY KEY,           -- AI 서버가 발급
  user_id             bigint      NOT NULL,
  record_card_id      bigint      NOT NULL,
  status              text        NOT NULL,       -- RECEIVED·UPLOADED·INVOKING·INVOKED·SUCCEEDED·FAILED
  failed_stage        text,                       -- INVOKE·INFERENCE
  endpoint_name       text        NOT NULL,

  input_json          jsonb       NOT NULL,       -- 요청 원본 그대로
  result_json         jsonb,                      -- 컨테이너 성공/실패 JSON 전체 (AI 내부 보관용. 완료 메시지는 4.1 형식으로 추출)
  error_code          text,                       -- 발행한 result.error.code (INFERENCE_FAILED 포함)
  error_message       text,                       -- 발행한 result.error.message (실패 파일이 JSON이 아니면 파일 내용 전체)

  input_s3_key        text        NOT NULL,
  output_location     text,                       -- 202 응답 헤더 X-Amzn-SageMaker-OutputLocation
  failure_location    text,                       -- 202 응답 헤더 X-Amzn-SageMaker-FailureLocation
  invoke_http_status  int,
  invoke_error_code   text,
  invoke_message      text,

  kafka_topic         text        NOT NULL,
  kafka_partition     int         NOT NULL,
  kafka_offset        bigint      NOT NULL,

  created_at          timestamptz NOT NULL DEFAULT now(),
  invoked_at          timestamptz,                -- INVOKING 기록 시각 (호출 직전, 무응답 점검 기준)
  completed_at        timestamptz,
  published_at        timestamptz,                -- 완료 메시지 발행 확인 시각
  UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);
CREATE INDEX ix_req_card    ON inference_request (record_card_id, created_at DESC);
CREATE INDEX ix_req_pending ON inference_request (status, invoked_at) WHERE status IN ('INVOKING', 'INVOKED');
```

- 사진 정보는 `input_json.photos`, 음악 키는 `result_json.photo_music`에 있다 → 사진 테이블 없음. 사진별 조회가 잦아지면 추가
- 환경별로 **DB를 따로** 둔다: `runiverse_ai_dev` · `runiverse_ai_test` · `runiverse_ai_prod`
- 마이그레이션은 `migrations/NNN_*.sql` 순번 파일. ORM 없이 SQL 직접 작성

### 3.2 상태 전이

```txt
RECEIVED ──업로드──▶ UPLOADED ──호출 직전──▶ INVOKING ──202──▶ INVOKED ──알림(성공)──▶ SUCCEEDED
   │                                            │                    └─알림(실패)──▶ FAILED (INFERENCE)
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
| `INVOKING` (호출됐는지 모름) | **다시 호출하지 않고** 커밋. 알림 또는 무응답 점검(4.3)에 맡긴다 |
| `INVOKED`·`SUCCEEDED` | 호출하지 않고 커밋 |
| `FAILED(INVOKE)`이고 `published_at` 없음 | 완료 메시지만 다시 발행 → 커밋 |
| `FAILED`이고 `published_at` 있음 | 커밋 |

- 사용자 재요청은 offset이 다르므로 새 요청으로 처리됨 (요청건대로 처리 원칙 유지)
- 오토 커밋은 끄고 수동 커밋만 사용

### 4.2 완료 (SQS → Kafka)

| 단계 | 처리 |
|---|---|
| 수신 | SQS long polling (20초) |
| 파싱 | SNS 봉투 → SageMaker 알림 → `inferenceId`, 성공·실패, 결과 위치 |
| 조회 | DB에서 `inference_id` 조회. `published_at`이 있으면 SQS 메시지만 삭제 |
| 읽기 | 성공: `OutputLocation` / 실패: `FailureLocation` |
| 조립 | Notion 4.1 형식으로 추출 (아래 표) |
| 저장 | `result_json`·`status`·`error_code`·`error_message`·`completed_at` |
| 발행 | 메시지 키 `record_card_id`. 발행 확인 → `published_at` → **SQS 메시지 삭제** |

**완료 메시지 `result` 조립 (Notion 4.1)**

| 경우 | `result` |
|---|---|
| 성공 | `base_music`의 `s3_key`·`duration_seconds`, `photo_music[]`의 `record_card_image_id`·`s3_key`·`duration_seconds`만 |
| 실패 파일이 JSON | `{error: {code, message}}`만 (`detail` 제외) |
| 실패 파일이 JSON 아님 | `{error: {code: INFERENCE_FAILED, message: 파일 내용 전체}}` |
| 실패 파일 없음 | `{error: {code: INFERENCE_FAILED, message: ...}}` |

- 완료 메시지 최상위 필드: `inference_id`·`record_card_id`·`status`·`failed_stage`·`invoke`·`result`. **`user_id`는 넣지 않음**
- 값은 바꾸거나 새로 계산하지 않는다 (`duration_seconds` 반올림 등 금지)

- 발행에 실패하면 SQS 메시지를 지우지 않음 → 가시성 타임아웃 후 다시 수신. 추론을 다시 돌리는 게 아니라 알림 전달만 다시 하는 것
- `maxReceiveCount`를 넘으면 DLQ로 이동
- DB에 없는 `inferenceId`(다른 환경 등): 에러 로그 후 SQS 메시지 삭제

### 4.3 무응답 (계획 9장 ③)

- 호출할 때 `InvocationTimeoutSeconds`·`RequestTTLSeconds`를 명시
- 시간이 초과되면 SageMaker가 실패 알림을 보내는지 dev 검증 ④에서 확인
  - 보냄 → 별도 로직 없음
  - 안 보냄 → 주기 점검: `INVOKED`이고 `invoked_at` 기준을 넘긴 행 → `INFERENCE_FAILED` 발행
- **`INVOKING`에 멈춘 행**(호출 전후에 서버 중단)은 알림이 와도 결과 위치를 DB에서 모른다
  - SNS 알림에 결과 위치가 있으면 알림 값을 쓴다 → Phase 6에서 실제 메시지로 확인
  - 없거나 알림이 안 오면 → 주기 점검 대상에 `INVOKING`도 포함해 `INFERENCE_FAILED` 발행
  - 그 전까지 완료 흐름은 결과 위치가 없는 알림을 SQS에 남긴다 (`maxReceiveCount` 넘으면 DLQ)

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
| `SQS_QUEUE_URL` | SNS 구독 큐 |
| `INVOCATION_TIMEOUT_SECONDS`·`REQUEST_TTL_SECONDS` | 4.3 |

- 기존 값(`KAFKA_ENV`·`AI_BUCKET_NAME`·`INPUT_PREFIX`·`ENDPOINT_NAME` 등)은 계획 문서 5장
- 잘못된 값이면 시작 시 실패

---

## 🛡️ 7. DB 서버 (EC2)

| 항목 | 기준 |
|---|---|
| 네트워크 | private 서브넷, 퍼블릭 IP 없음 |
| 보안 그룹 | 5432는 AI 서버에서만 허용 |
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
| 2.5 DB | 마이그레이션, Postgres Repository | 로컬 Docker PG: 상태 전이, 중복 offset, `published_at` 멱등 | — | — |
| 3. Kafka | Kafka 어댑터, 수동 커밋, `main` | **검증 ②** `KAFKA_ENV=dev`: 소비·발행, 재시작 후 재소비 | — | — |
| 4. S3 + 호출 | S3·SageMaker 어댑터 | 없는 엔드포인트로 호출 거부 → `INVOKE` 발행 | IAM 권한 | — |
| 4.5 DB 서버 | EC2 DB 연결 | 연결·마이그레이션 적용 | EC2·보안 그룹·EBS | **시작** |
| 5. 알림 인프라 | — | SNS 2개 + SQS + DLQ, 엔드포인트 Config 교체 | SNS·SQS·SageMaker | **시작** |
| 6. 스모크 | 실제 알림·실패 파일 수집 → `notification.py` 확정 | 컨테이너 단독 스모크 → **검증 ③** | 엔드포인트 테스트 | 지속 |
| 7. 실패 경로 | 타임아웃 파라미터, 4.3 결정 | **검증 ④** 400·거부·실패 파일·시간 초과 | 끝나면 엔드포인트 정리 | — |
| 8. 통합 | Dockerfile, 문서 갱신 | 백엔드 통합 테스트 | 배포 위치 | — |

- Phase 6 전에는 SNS 필드를 추측하지 않고 Fake 알림 형식으로만 작성

---

## 📝 9. CLAUDE.md 수정안 (✅ 2026-11-14 반영)

| 위치 | 현재 | 수정안 |
|---|---|---|
| 2장 ① 중복 | 요청 ID가 없어 **중복 판별은 하지 않는다** | Kafka `(topic, partition, offset)`으로 같은 메시지 재수신만 감지한다. 재요청은 offset이 달라 요청건대로 처리 |
| 1장 AI-SRV-07 | 보관 방식 미정 | AI 전용 DB(EC2 PostgreSQL) `inference_request` |
| 3장 저장소 구조 | 미정 | Python 3.11 · confluent-kafka · boto3 · psycopg 3. 이 문서 2장 |
| Don't | 회고 원문·사진 캡션·입력 JSON 본문을 로그·**로컬 저장소**에 남기기 | 로그·**로컬 파일**에 남기기. 원본 보관은 AI DB `input_json`에만 |
| Docs 표 | — | `docs/AI_서버_아키텍처_설계.md` 추가 |
