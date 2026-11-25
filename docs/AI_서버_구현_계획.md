# Runiverse AI 서버 — 구현 계획

> 백엔드(Kafka)와 SageMaker Async 엔드포인트 사이를 중계하는 **AI 전용 서버**의 범위·계약·결정 사항 정리.
> 작성 2026-11-13. 음악 생성 컨테이너는 별도 저장소 `C:\dev\runiverse\ai`(`runiverse-ai`)에 있다.
> 코드 구조·구현 방법은 이 문서에서 정하지 않는다. 진행하면서 개발자가 정한다.

---

## 📚 0. 정본 문서

| 주제 | 위치 |
|---|---|
| 요청 흐름·서버별 책임·Kafka 접속 | Notion「AI 음악 생성 요청 파이프라인」 |
| 입력 JSON·컨테이너 결과 JSON·Kafka 완료 메시지·오류 코드 | Notion「AI 기능 아키텍처 및 data 입출력」 3장·4장 |
| 버킷·S3 경로·보관 기준 | Notion「S3 버킷 저장 구조」 |
| 컨테이너 입력 검증 규칙 | `ai/src/runiverse_music/schemas.py`, `ai/docs/AI_음악_전처리_모듈.md` |

- 이 문서와 Notion이 다르면 **Notion이 우선**한다
- 세 문서 모두 2026-11-13에 사진 ID(`record_card_image_id`)·새 음악 경로(`{record_card_image_id}.wav`) 반영

---

## 🎯 1. 역할과 범위

| 한다 | 하지 않는다 |
|---|---|
| 요청 토픽 소비 | 음악 생성·분석 (컨테이너 몫) |
| `inference_id`(UUID) 생성, 입력 JSON을 S3 `async/inputs/`에 업로드 | 입력 내용 해석·변환 (그대로 전달) |
| `InvokeEndpointAsync` 호출 | **자동 재시도** (사용자 재요청 → 새 메시지) |
| 완료 감지 (S3 결과·실패 파일 주기 확인) | 카드·음악 상태 저장 (백엔드 DB 몫) |
| S3 결과·실패 JSON 읽기 | `audio/` 음악 파일 읽기·검증 (백엔드 몫) |
| 완료 토픽 발행 | presigned URL 발급 |

- 요청 단위: **카드 1건 = Kafka 메시지 1건 = Async 호출 1건**. 사진별로 쪼개지 않는다
- 백엔드는 Kafka 완료 메시지 하나만 읽는다. S3 `async/`를 읽지 않는다

---

## 🔀 2. 처리 흐름

```txt
[요청 토픽] {env}.record-card.music.requested
   ▼ ① 소비
   │     └ JSON 아님·ID 누락 → ⑦로 바로 (failed_stage=INVOKE, http_status=null, error_code=INVALID_MESSAGE, inference_id=uuid5(Kafka 위치))
   ▼ ② inference_id = UUID
   ▼ ③ 입력 JSON → s3://{AI_BUCKET}/async/inputs/{env}/{record_card_id}/{inference_id}.json
   ▼ ④ InvokeEndpointAsync(EndpointName, InputLocation, InferenceId=inference_id, ContentType=application/json)
   │     ├ 거부(400·500·503) 또는 응답 없음(http_status=null) → ⑦로 바로 (failed_stage=INVOKE, result=실패 양식)
   │     └ 202 → inference_id ↔ record_card_id, OutputLocation·FailureLocation 보관
   ▼ (컨테이너 처리, 수 분)
   ▼ ⑤ 결과·실패 파일 주기 확인 (S3, 2026-11-16 SNS·SQS 대신)
   ▼ ⑥ 성공: OutputLocation JSON 읽기 / 실패: FailureLocation JSON 읽기
   ▼ ⑦ 완료 메시지 발행
[완료 토픽] {env}.record-card.music.generated
```

---

## 📥 3. 입력: 요청 토픽

컨테이너 입력 JSON (Notion 아키텍처 문서 3장). AI 서버는 **수정 없이 S3에 올린다.**

```json
{
  "user_id": 123,
  "record_card_id": 101,
  "memo": "힘들었지만 끝까지 달려서 뿌듯했다",
  "activity_type": "TRAVEL",
  "running_summary": { "avg_pace": 360, "weather_code": 61, "...": "나머지 지표" },
  "photos": [
    { "record_card_image_id": 201, "image_key": "cards/101/photos/201.jpg", "caption": "바닷가에서 찍은 사진" }
  ]
}
```

- 컨테이너는 모르는 필드를 **400으로 거부**한다 (`extra=forbid`, `running_summary`만 예외). 메시지에 요청 ID 등 다른 필드가 붙어 오면 S3에 올리기 전에 떼야 한다
- ✅ **메시지 본문 = 위 입력 JSON 그대로** (봉투·`generation_id` 없음, 2026-11-13 백엔드 협의)
- 신규 요청과 재요청(업데이트)은 같은 형식이다. **AI 서버는 둘을 구분하지 않고 요청건대로 처리**한다. 업데이트 여부는 백엔드가 `record_card_id`·사진 ID로 스스로 판단
- 입력 검증은 컨테이너가 한다. AI 서버는 JSON 파싱과 `user_id`·`record_card_id` 존재 정도만 본다 (S3 경로·완료 메시지에 필요)

---

## 📤 4. 출력: 완료 토픽

```json
{
  "inference_id": "3f2a9c1e-...",
  "record_card_id": 101,
  "status": "SUCCEEDED",
  "failed_stage": null,
  "invoke": { "http_status": 202, "error_code": null, "message": null },
  "result": {
    "base_music": { "s3_key": "audio/test/123/TRAVEL/101/base.wav", "duration_seconds": 30.0 },
    "photo_music": [
      { "record_card_image_id": 201, "s3_key": "audio/test/123/TRAVEL/101/201.wav", "duration_seconds": 10.0 }
    ]
  }
}
```

| 경우 | `status` | `failed_stage` | `invoke.http_status` | `result` |
|---|---|---|---|---|
| 성공 | `SUCCEEDED` | `null` | 202 | 출력 위치 JSON(4.2.1)에서 `base_music`의 `s3_key`·`duration_seconds`, `photo_music[]`의 `record_card_image_id`·`s3_key`·`duration_seconds` 추출 |
| 컨테이너 실패 | `FAILED` | `INFERENCE` | 202 | 실패 양식. `error`=실패 위치 JSON(4.2.2)의 `{code, message}` |
| 컨테이너 응답 없음 (중단·시간 초과) | `FAILED` | `INFERENCE` | 202 | 실패 양식. `error.code=INFERENCE_FAILED`. 실패 파일이 JSON이 아니면 `message`=파일 내용 전체 |
| 호출 거부 | `FAILED` | `INVOKE` | 400·500·503 | 실패 양식. SageMaker 오류 이름·메시지를 `invoke`와 `error`에 |
| SageMaker 응답 없음 (S3 업로드 실패·네트워크 오류) | `FAILED` | `INVOKE` | `null` | 실패 양식. 원인은 `invoke.error_code`·`message` (`error`에도) |
| 요청 메시지 오류 (JSON 아님·ID 누락) | `FAILED` | `INVOKE` | `null` | 실패 양식. `error.code=INVALID_MESSAGE`. `inference_id`=Kafka 위치로 만든 uuid5, `record_card_id`는 읽히면 담고 아니면 `null` |

- 실패 `result` = 성공 양식에 `null` + `error`: `{base_music: {s3_key: null, duration_seconds: null}, photo_music: [{record_card_image_id: 요청 값, s3_key: null, duration_seconds: null}], error: {code, message}}` (2026-11-19 백엔드 요청). `photo_music`은 요청 `photos[]` 순서(DB `input_json`에서 읽음), 읽을 수 없으면 `[]`
- **모든 완료 메시지에 `inference_id`** (본문 + Kafka 헤더 `id`, 2026-11-25 백엔드 요청으로 헤더 이름 변경)

- `generation_id`는 없다. 백엔드는 `record_card_id`·사진 ID로 결과를 자기 요청과 연결한다
- **AI 출력(컨테이너 결과 전체)과 완료 메시지는 형식이 다르다.** `user_id`·`review_mood`·`photo_index`·`image_key`·`manifest`·`error.detail`은 넣지 않는다 (S3 `async/outputs/`·`failures/`에만, 7·14일 만료)
- 컨테이너 `error.code`: `INVALID_REQUEST`(400) · `MUSIC_GENERATION_OOM`(500) · `MUSIC_GENERATION_FAILED`(500) · `MUSIC_UPLOAD_FAILED`(502) · `INTERNAL_ERROR`(500)

---

## ⚙️ 5. 설정 (환경 변수 후보)

| 값 | 로컬 | 테스트 | 운영 |
|---|---|---|---|
| `KAFKA_ENV` | `dev` | `test` | `prod` |
| `KAFKA_PASSWORD` | 환경 변수로만 전달. **코드·문서·로그에 쓰지 않음** | | |
| `AI_BUCKET_NAME` | `<AI_BUCKET_TEST>` | `<AI_BUCKET_TEST>` | `<AI_BUCKET_PROD>` |
| `INPUT_PREFIX` | `async/inputs/local` | `async/inputs/test` | `async/inputs/prod` |
| `ENDPOINT_NAME` | `runiverse-music-test-v1` | `runiverse-music-test-v1` | `runiverse-music-prod-v1` |
| 리전 | `ap-northeast-2` | 동일 | 동일 |

- ⚠️ **개발에는 `KAFKA_ENV=dev`만** 쓴다. `test`는 테스트 서버와 메시지가 겹친다
- Kafka 접속 (백엔드 공유값)
  - `bootstrap.servers` `<DB_SERVER_IP>:9092`, `SASL_SSL` + `SCRAM-SHA-512`
  - 사용자 `{env}.runiverse-ai`, 컨슈머 그룹 `{env}.runiverse-ai.record-card-music`
  - CA 인증서 `config/local/kafka-ca.crt` (실행 디렉터리 기준. 저장소에 커밋할지 확인)
- 엔드포인트 이름·버킷은 코드에 하드코딩하지 않고 설정으로 받는다. 잘못된 값이면 시작 시 실패

---

## 🔗 6. 상태 연결·중복·타임아웃

**`inference_id` ↔ 요청 정보 보관** (9장 ②)
- ✅ **AI 전용 DB에 저장**으로 결정 (2026-11-13, 2026-11-16 PostgreSQL → MySQL 8.4). 상세는 `AI_서버_아키텍처_설계.md` 3장. 아래 후보는 검토 기록
- 결과 위치(202 응답)와 `record_card_id`를 함께 보관해야 완료 메시지를 만들 수 있다
- 후보
  - 메모리: 가장 단순. 서버 재시작 시 진행 중 요청을 잃음
  - Redis·DB: 재시작에도 유지. 구성 요소 추가
  - S3 입력 경로에서 역산: 경로에 `record_card_id`가 있음 (SNS 알림에 입력 위치가 오는지 확인 필요). 결과 JSON에도 `record_card_id`가 있음
- AI 서버는 카드 상태를 저장하지 않는다. 보관하는 것은 **완료 메시지를 만들기 위한 연결 정보뿐**

**중복 소비**
- Kafka는 같은 메시지를 두 번 줄 수 있다. 요청 ID가 없어 **내용으로는** 재요청과 구분할 수 없다
- ✅ 같은 메시지 재수신만 DB의 `(topic, partition, offset)`으로 감지해 호출을 건너뛴다 (2026-11-13). 재요청은 offset이 달라 요청건대로 처리. 상세는 `AI_서버_아키텍처_설계.md` 4.1
- 오프셋 커밋 시점: 호출 수락(202) 이후. 커밋 전에 죽으면 재소비되어 같은 카드를 한 번 더 호출할 수 있다 (비용·시간만 추가)

**응답이 오지 않을 때** (9장 ③)
- SageMaker 기본값: 처리 제한 900초, 대기열 체류 제한 21,600초(6시간). `InvokeEndpointAsync`의 `InvocationTimeoutSeconds`·`RequestTTLSeconds`로 조정 가능
- 기준 시간이 지나도 알림이 없으면 `INFERENCE_FAILED`로 발행할지, 백엔드가 판단할지 정해야 한다

---

## 🛡️ 7. 권한·보안·로그

| 대상 | 필요한 권한 |
|---|---|
| AI 서버 실행 주체 | `async/inputs/{env}/*` 쓰기 · `async/outputs/{env}/*`·`async/failures/{env}/*` 읽기 · `sagemaker:InvokeEndpointAsync` · 결과 prefix `s3:ListBucket` (없는 파일이 404로 오게) |
| SageMaker 실행 Role | 이미 구성됨 (사진 버킷 읽기, `audio/*` 쓰기) |

- **AWS 리소스 생성·변경은 개발자가 Console에서 직접** 한다 (`ai` 저장소와 같은 원칙)
- SNS·SQS를 쓰지 않으므로 엔드포인트 Config에 알림 토픽을 지정하지 않는다 (2026-11-16)
- **회고 원문·사진 캡션을 로그에 남기지 않는다** (개인정보). 로그 키: `inference_id`·`record_card_id`·단계·소요 시간·오류 코드
- 입력 JSON(회고 원문 포함)은 `async/inputs/`에 7일 보관 후 만료 (테스트 버킷 수명 주기 규칙)

---

## 🧪 8. 검증 순서

| 단계 | 위치 | 확인할 것 |
|---|---|---|
| ① 단위 테스트 | 로컬 | 메시지 파싱, S3 키 생성, 완료 메시지 조립 (4장 표의 4가지 경우) |
| ② Kafka 연동 | 로컬 + `KAFKA_ENV=dev` | 소비·발행, 오프셋 커밋, 재시작 후 재소비 |
| ③ AWS 연동 | dev 엔드포인트 (**과금**) | 입력 업로드 → 호출 202 → 결과 파일 확인 → 완료 발행 |
| ④ 실패 경로 | dev 엔드포인트 | 잘못된 입력(400), 없는 엔드포인트 이름(호출 거부), 실패 위치 파일 형식 |

- ③ 전에 컨테이너 단독 스모크(Console에서 S3 입력 → 엔드포인트 테스트)를 먼저 통과시킨다
- ⚠️ **확인 필요**
  - 실패 위치 파일이 컨테이너 JSON 그대로인지, SageMaker가 감싸는지
  - 시간 초과 시 SageMaker가 실패 파일을 남기는지
  - → ③·④에서 실제 파일을 보고 확인한다

---

## 📌 9. 결정이 필요한 것

| # | 항목 | 내용 | 협의 대상 |
|---|---|---|---|
| ① | ~~요청 메시지 형태~~ | ✅ 입력 JSON 그대로, `generation_id` 없음 (2026-11-13) | 백엔드 |
| ② | ~~연결 정보 보관~~ | ✅ AI 전용 DB(MySQL 8.4, 2026-11-16 전환) `inference_request`. 입력·결과 원본도 저장 (2026-11-13) | AI |
| ③ | 무응답 타임아웃 | 기준 시간과 `INFERENCE_FAILED` 발행 주체 | AI + 백엔드 |
| ④ | ~~SNS 수신 방식~~ | ✅ SNS·SQS 쓰지 않음. 202 응답의 결과 위치를 S3에서 주기적으로 확인 (2026-11-16, 2026-11-13 SNS→SQS 결정 대체) | AI |
| ⑤ | ~~파티션 키~~ | ✅ 완료 메시지 키 = `record_card_id`(문자열). `null`이면 키 없이 발행. Notion 4.1 반영 (2026-11-14) | 백엔드 |
| ⑥ | 기술 스택·배포 위치 | ✅ 스택: Python 3.11 · confluent-kafka · boto3 · PyMySQL (2026-11-16). test는 백엔드 test 서버 EC2에 같이 배포, AI DB는 그 서버의 MySQL 컨테이너 (2026-11-16). ⏳ AI 서버 실행 환경(EC2·ECS 등)·AWS 자격 증명 방식은 미정 | AI |
| ⑦ | ~~운영 환경 값~~ | ✅ `KAFKA_ENV=prod`, 엔드포인트 `runiverse-music-prod-v1` (2026-11-25) | 백엔드 + AI |
| ⑧ | 메시지 버전 필드 | 스키마 변경 대비 `version` 필드 여부 | 백엔드 + AI |
| ⑨ | ~~계약에 없는 실패~~ | ✅ S3 업로드 실패·응답 없는 호출·요청 메시지 오류 → `INVOKE` + `http_status=null`. Notion 3장·4.1 반영 (2026-11-14) | 백엔드 + AI |

---

## 🗺️ 10. 진행 순서 (제안)

> 확정된 작업 순서는 `AI_서버_아키텍처_설계.md` 8장. 아래는 초기 제안 기록

1. 9장 ⑥ 결정 → 저장소 생성
2. Kafka 소비·발행만 먼저 (AWS 없이, 받은 메시지를 로그 대신 ID만 출력)
3. S3 업로드 + `InvokeEndpointAsync` → 호출 거부 경로 완료 메시지까지
4. 결과·실패 파일 주기 확인 → 성공·실패 완료 메시지 (SNS·SQS 없음, 2026-11-16)
5. 연결 정보 보관(9장 ②)·타임아웃(9장 ③)
6. dev 엔드포인트로 8장 ③·④ 검증 후 백엔드와 통합 테스트
