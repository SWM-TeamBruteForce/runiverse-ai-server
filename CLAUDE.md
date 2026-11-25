# Project: Runiverse AI Server

Runiverse 앱의 **AI 음악 생성 요청을 중계**하는 서버 저장소.
백엔드(Kafka)에서 기록 카드 요청을 받아 SageMaker Async 엔드포인트를 호출하고, 완료 결과를 다시 Kafka로 돌려준다.
음악 생성 자체는 하지 않는다 (컨테이너 저장소 `../ai` 몫).
개발자는 **AWS SageMaker를 처음 쓴다.** AWS 작업은 CLI·스크립트가 아니라 **AWS Console에서 직접** 한다.

## 1. 주요 기능

| ID | 기능 | 핵심 |
|---|---|---|
| AI-SRV-01 | 요청 소비 | `{env}.record-card.music.requested` 소비. 본문 = 컨테이너 입력 JSON 그대로 |
| AI-SRV-02 | 입력 업로드 | `inference_id`(UUID) 생성 → `async/inputs/{env}/{record_card_id}/{inference_id}.json` |
| AI-SRV-03 | Async 호출 | `InvokeEndpointAsync(InputLocation, InferenceId=inference_id)` |
| AI-SRV-04 | 완료 감지 | 202 응답의 `OutputLocation`·`FailureLocation`을 S3에서 주기적으로 확인 (SNS·SQS 안 씀, 2026-11-16) |
| AI-SRV-05 | 결과 읽기 | 성공 `OutputLocation`, 실패 `FailureLocation`의 JSON 읽기 |
| AI-SRV-06 | 완료 발행 | `{env}.record-card.music.generated`에 `status`·`failed_stage`·`invoke`·`result` 발행. `result`는 컨테이너 결과에서 **필요한 값만 추출** |
| AI-SRV-07 | 연결 정보 보관 | `inference_id` ↔ `record_card_id`. AI 전용 DB(MySQL 8.4) `inference_request`에 입력·결과 원본과 함께 저장 |

- **카드 1건 = Kafka 메시지 1건 = Async 호출 1건**. 사진별로 쪼개지 않는다
- **`generation_id`는 없다** (2026-11-13 백엔드 협의). 신규 요청과 재요청(업데이트)은 같은 형식이고 **구분하지 않고 요청건대로 처리**한다. 업데이트 여부는 백엔드가 `record_card_id`·사진 ID로 판단
- 백엔드는 완료 메시지 하나만 읽는다. S3 `async/`를 읽지 않는다
- **구현 현황** (2026-11-18): Phase 0~8 완료 (흐름·Fake·MySQL·Kafka·S3·SageMaker 어댑터·워커/서버·`main`, 실패 경로 검증, test EC2 배포 후 백엔드 요청으로 성공·실패 발행 확인). 다음은 사진 버킷 수정 후 재확인, 운영 배포(CI/CD) (Phase 5 SNS·SQS는 취소). 계획은 `docs/AI_서버_구현_계획.md`, 구조·DB·작업 순서는 `docs/AI_서버_아키텍처_설계.md`

## 2. 처리 단계별 핵심사항

> 메시지·JSON 형식의 정본은 Notion「AI 기능 아키텍처 및 data 입출력」3·4장. 아래는 요약이다.

요청 1건 처리 순서: **소비 → `inference_id` 생성 → S3 업로드 → 호출 → (컨테이너 처리) → 결과 파일 확인 → 완료 발행**.

**① 요청 소비**
- 본문은 컨테이너 입력 JSON: `user_id` `record_card_id` `memo` `activity_type` `running_summary` `photos[]`(`record_card_image_id` `image_key` `caption`)
- **수정 없이 S3에 올린다.** 컨테이너는 모르는 필드를 400으로 거부한다 (`running_summary`만 예외)
- 입력 검증은 컨테이너 몫. AI 서버는 JSON 파싱과 `user_id`·`record_card_id`만 본다
- 백엔드 ID(`user_id`·`record_card_id`·`photos[].record_card_image_id`)는 **UUID 문자열** (2026-11-17). UUID 모양(8-4-4-4-12 16진수)만 받고, 받은 값 그대로 저장·발행 (대소문자 변환 없음). DB는 `CHAR(36)`
- 파싱 실패·ID 누락 → 호출 없이 실패 발행 (`failed_stage=INVOKE`, `invoke.http_status=null`, `invoke.error_code=INVALID_MESSAGE`). `inference_id`는 Kafka 위치(topic·partition·offset)로 만든 고정 UUID(uuid5, 같은 메시지 재수신 시 같은 값), DB 기록 없음. `record_card_id`는 본문에서 읽히면 담고 아니면 `null`
- Kafka는 같은 메시지를 두 번 줄 수 있다. **같은 메시지 재수신만** DB의 `(topic, partition, offset)`으로 감지해 호출을 건너뛴다. 재요청은 offset이 달라 요청건대로 처리한다 (요청 ID가 없어 내용으로 중복을 판별하지 않음)

**② 입력 업로드·호출**
- `inference_id`는 **AI 서버가 호출 전에 생성**한다 (S3 경로에 필요, SageMaker 자동 할당 값을 쓰지 않음)
- 호출 거부(400·500·503) → 컨테이너 실행 없이 바로 실패 발행 (`failed_stage=INVOKE`, `result.error`=`invoke`의 코드·메시지)
- SageMaker 응답 없음(S3 업로드 실패·네트워크 오류·시간 초과) → `invoke.http_status=null`로 같은 실패 발행
- 202 → `inference_id`와 응답 헤더의 `OutputLocation`·`FailureLocation` 보관 후 결과 파일 대기. 오프셋은 202 이후 커밋

**③ 완료 감지·결과 읽기**
- 완료 워커가 `COMPLETION_POLL_SECONDS`(기본 10초)마다 **발행 전 행**을 조회해 `OutputLocation` → 없으면 `FailureLocation`을 S3에서 읽는다. 둘 다 없으면 다음에 다시
- 제한 시간(`INVOCATION_TIMEOUT_SECONDS`+`REQUEST_TTL_SECONDS`)까지 둘 다 없으면 응답 없음 → `error.code=INFERENCE_FAILED`. `INVOKING`에 멈춘 행도 같다
- 결과 파일 이름은 SageMaker UUID → 202 응답의 위치를 DB에 저장해 그대로 읽는다
- ⚠️ 실행 주체에 결과 prefix `s3:ListBucket`이 없으면 없는 파일도 403 → "아직 없음"을 구분 못 한다
- 성공 → `OutputLocation` JSON에서 `base_music`의 `s3_key`·`duration_seconds`, `photo_music[]`의 `record_card_image_id`·`s3_key`·`duration_seconds`만 추출
- 실패 → `FailureLocation` JSON에서 `error.code`·`message`만 추출. JSON이 아니면 `error.code=INFERENCE_FAILED`, `message`=파일 내용 전체. 파일이 없으면 `error.code=INFERENCE_FAILED`
- 컨테이너 결과 전체(AI 출력)와 Kafka 완료 메시지는 **형식이 다르다**. 나머지(`user_id`·`review_mood`·`photo_index`·`image_key`·`manifest` 등)는 S3에만 남는다
- 실패 파일 (2026-11-18 dev 확인): 컨테이너 오류는 **컨테이너 JSON 그대로** (SageMaker가 감싸지 않음). `InvocationTimeoutSeconds` 초과는 **JSON이 아닌 한 줄** (`Timed out uploading object (bucket: …, key: …)`) → `INFERENCE_FAILED`, `message`=그 문장. 실제 파일은 `tests/test_messages.py`에 고정
- 시간 초과에도 실패 파일이 남으므로 "응답 없음" 판정은 결과·실패 파일이 모두 없을 때만 쓰인다

**④ 완료 발행**

| 경우 | `status` | `failed_stage` | `invoke.http_status` | `result` |
|---|---|---|---|---|
| 성공 | `SUCCEEDED` | `null` | 202 | `{base_music: {s3_key, duration_seconds}, photo_music: [{record_card_image_id, s3_key, duration_seconds}]}` |
| 컨테이너 실패 | `FAILED` | `INFERENCE` | 202 | 실패 양식, `error`=실패 파일의 `{code, message}` |
| 컨테이너 응답 없음 | `FAILED` | `INFERENCE` | 202 | 실패 양식, `error.code=INFERENCE_FAILED` |
| 호출 거부 | `FAILED` | `INVOKE` | 400·500·503 | 실패 양식, `error`=`invoke.error_code`·`message` |
| SageMaker 응답 없음 | `FAILED` | `INVOKE` | `null` | 실패 양식, `error`=`invoke.error_code`·`message` |
| 요청 메시지 오류 | `FAILED` | `INVOKE` | `null` | 실패 양식, `error.code=INVALID_MESSAGE` |

- 실패 `result` = 성공 양식에 `null` + `error`: `{base_music: {s3_key: null, duration_seconds: null}, photo_music: [{record_card_image_id: 요청 값, s3_key: null, duration_seconds: null}], error: {code, message}}` (2026-11-19 백엔드 요청). `photo_music`은 요청 `photos[]` 순서, 읽을 수 없으면 `[]`
- 식별자: `inference_id`·`record_card_id` (`user_id`는 넣지 않음). **모든 완료 메시지에 `inference_id`가 있다**
- Kafka: 키 `record_card_id`(없으면 키 없음), **헤더 `id`**(값 = 본문 `inference_id`, 백엔드 중복 처리용)
- 컨테이너 `error.code`: `INVALID_REQUEST`(400) `MUSIC_GENERATION_OOM`(500) `MUSIC_GENERATION_FAILED`(500) `MUSIC_UPLOAD_FAILED`(502) `INTERNAL_ERROR`(500)

**오류 원칙**
- **자동 재시도하지 않는다.** 실패는 완료 메시지로 알리고, 사용자가 재요청하면 새 메시지로 들어온다
- boto3 SageMaker 클라이언트의 기본 재시도를 끈다 (`max_attempts=1`)
- 발행 실패 시 `published_at`이 비어 있어 다음 확인 때 다시 발행하는 것은 허용한다 (전달 재시도). 추론을 다시 돌리지 않는다
- 결과 JSON은 정해진 필드만 추출한다. 값을 바꾸거나 새로 계산하지 않는다
- 무응답 기준 시간·발행 주체는 미정 (SageMaker 기본: 처리 900초, 대기열 6시간)

## 3. 저장소 구조

- 언어·라이브러리: **Python 3.11** · `confluent-kafka` · `boto3` · `pydantic-settings` · `PyMySQL` · `pytest` (2026-11-13, DB 드라이버는 2026-11-16 `psycopg` → `PyMySQL`)
- 실행: 단일 프로세스 + 워커 스레드 2개(요청·완료). 웹 프레임워크 없음
- AI DB: **MySQL 8.4** 컨테이너, 환경별 DB 분리 (`runiverse_ai_dev`·`_test`·`_prod`). 2026-11-16 PostgreSQL에서 전환
- AI 서버 배포 위치 (2026-11-18): **AI 서버 전용 EC2**에 Docker 컨테이너. test는 `<AI_SERVER_EC2>`(<AI_SERVER_IP>, 임시), 운영은 CI/CD 구성 때 운영 EC2를 새로 만든다
- AI DB 위치: test는 백엔드 test 서버 EC2(`<DB_SERVER_EC2>`, <DB_SERVER_IP>)의 AI 전용 MySQL 컨테이너, 포트 3307. 운영은 미정
- AWS 자격 증명: EC2 인스턴스 역할 (test `<AI_SERVER_TEST_ROLE>`). 액세스 키를 쓰지 않는다
- 접속: 배스천 경유 SSH (`~/.ssh/config`의 `runiverse-ai-test`). 구축 절차는 Notion「AI 서버 AWS Console 구축 가이드」
- 패키지 구조는 `docs/AI_서버_아키텍처_설계.md` 2장
- 리전 서울 `ap-northeast-2`

| 경로 | 내용 |
|---|---|
| `docs/` | 구현 계획 (`AI_서버_구현_계획.md`), 아키텍처 설계 (`AI_서버_아키텍처_설계.md`) |
| `src/runiverse_ai_server/` | 서버 코드 (`adapters/`: kafka·mysql·s3·sagemaker) |
| `migrations/` | DB 마이그레이션 SQL (`NNN_*.sql`). MySQL은 DDL을 되돌리지 못함 → 파일 하나에 DDL은 되도록 하나씩 |
| `tests/` | 단위·흐름 테스트, Fake (`fakes.py`), Fake·MySQL 공용 계약 테스트 |
| `scripts/` | 점검 스크립트 (SageMaker 스모크, Kafka 점검·소비 확인·메시지 구조 확인, 호출 거부 확인) |
| `Dockerfile` · `docker/` | AI 서버 이미지 (비root, `KAFKA_PASSWORD_FILE`·`DB_PASSWORD_FILE`로 비밀번호를 파일에서 읽음) |
| `deploy/test/` | test 배포 스크립트(`deploy.sh`), 설정 예시(`server.env.example`. 실제 `server.env`는 git 제외) |
| `config/local/kafka-ca.crt` | Kafka CA 인증서 (실행 디렉터리 기준, git 제외) |

**환경 설정** (값은 설정으로 받고 잘못되면 시작 시 실패)

| 값 | 로컬 | 테스트 | 운영 |
|---|---|---|---|
| `KAFKA_ENV` | `dev` (백엔드 연동 확인 시 `test`) | `test` | 미정 |
| `AI_BUCKET_NAME` | `<AI_BUCKET_TEST>` | `<AI_BUCKET_TEST>` | `<AI_BUCKET_PROD>` |
| `INPUT_PREFIX` | `async/inputs/local` | `async/inputs/test` | `async/inputs/prod` |
| `ENDPOINT_NAME` | `runiverse-music-test-v1` | `runiverse-music-test-v1` | 미정 |

- Kafka: `<DB_SERVER_IP>:9092`, `SASL_SSL` + `SCRAM-SHA-512`, 사용자 `{env}.runiverse-ai`, 그룹 `{env}.runiverse-ai.record-card-music`
- `KAFKA_ENV`는 `dev`·`test`만 허용 (2026-11-17). 백엔드 요청 이벤트는 **test 토픽**으로 나온다 (dev 계정은 test 권한 없음)
- ⚠️ test 서버에 AI 서버가 떠 있으면 로컬에서 `KAFKA_ENV=test`로 **서버를 켜지 않는다** (같은 그룹이라 요청이 나뉘고 완료가 두 번 나갈 수 있음). 점검 스크립트(`kafka_check`·`kafka_shape_check`)는 커밋하지 않아 괜찮다
- 로컬에서 Kafka 접속: WSL + 배스천 SSH 터널 (`lo`에 `<DB_SERVER_IP>` 등록). 절차는 Notion「Kafka WSL 접속 방법 가이드」
- `KAFKA_PASSWORD`·`DB_PASSWORD`는 **환경 변수로만** 받는다
- 추가 설정: `DB_HOST`·`DB_PORT`(기본 3306)·`DB_NAME`·`DB_USER`, `COMPLETION_POLL_SECONDS`(기본 10), `INVOCATION_TIMEOUT_SECONDS`·`REQUEST_TTL_SECONDS` (기본 900·21600)

## 4. 지침

### Commands

```bash
uv venv --python 3.11 .venv && uv pip install --python .venv -e ".[dev]"   # 설치 (WSL은 ~/.venvs/ai-server)
PYTHONUTF8=1 .venv/Scripts/python -m pytest -q                              # 단위 테스트 (MySQL 테스트는 건너뜀)
TEST_DATABASE_URL=mysql://runiverse_ai:<로컬 비밀번호>@127.0.0.1:53306/runiverse_ai_pytest   PYTHONUTF8=1 .venv/Scripts/python -m pytest -q                            # 로컬 Docker mysql:8.4 포함
DB_HOST=... DB_PORT=... DB_NAME=... DB_USER=... DB_PASSWORD=... python -m runiverse_ai_server.migrate   # 마이그레이션
```

- `TEST_DATABASE_URL`은 테이블을 지우고 다시 만든다 → **전용 테스트 DB만** 가리킨다
- 서버 실행: 저장소 루트에서 `python -m runiverse_ai_server.main` (설정은 환경 변수로만). 종료 코드 0 정상 · 1 워커 오류(재시작하면 커밋 위치부터) · 2 설정 오류
- test 배포 (WSL, 저장소 루트): `bash deploy/test/deploy.sh build` → `check` → `migrate` → `run`, 로그는 `status`. **커밋된 코드만** 배포되고, `check`·`migrate`·`run`은 서버의 마지막 빌드 이미지를 쓴다
- `*.sh`는 LF로 저장한다 (CRLF면 서버에서 `set: pipefail: invalid option name`)
- ⚠️ 서버를 켜면 요청 토픽의 쌓인 메시지를 처리하고 **완료 토픽에 실제로 발행**한다. 먼저 `scripts/kafka_check.py`로 읽을 메시지 수를 확인한다

### AWS 작업 (Console)

- S3·IAM·SageMaker·EC2·CloudWatch 작업은 **개발자가 Console에서 직접** 한다. Claude가 AWS CLI·boto3·스크립트로 대신 실행하지 않는다 (서버 코드가 런타임에 하는 호출은 예외)
- Claude는 **Console 단계별 안내**를 준다: 서비스 → 메뉴 경로 → 입력할 필드와 값 → 완료 확인 방법
- 처음 쓰는 개념(IAM 역할, 버킷 정책, Async 결과 위치 등)은 단계마다 1~2줄로 무엇인지와 왜 필요한지 붙인다
- 리전은 항상 서울 `ap-northeast-2`인지 먼저 확인시킨다
- 과금이 시작·지속되는 단계(엔드포인트 생성·유지)는 미리 알리고, 끝나면 삭제 방법도 함께 안내한다
- 엔드포인트·컨테이너 설정의 정본은 `../ai/deploy/envs/*.yaml`

### 검증 단계

앞 단계를 통과하기 전에 다음으로 가지 않는다.

| 단계 | 위치 | 잡는 것 |
|---|---|---|
| ① 단위 테스트 | 로컬 | 메시지 파싱, S3 키, 완료 메시지 조립(④ 표 4가지) |
| ② Kafka 연동 | 로컬 + `KAFKA_ENV=dev` | 소비·발행, 오프셋 커밋, 재시작 후 재소비 |
| ③ AWS 연동 | dev 엔드포인트 (**과금**) | 업로드 → 호출 202 → 결과 파일 확인 → 발행 |
| ④ 실패 경로 | dev 엔드포인트 | 잘못된 입력(400), 호출 거부, 실패 파일 형식 |

- ③ 전에 컨테이너 단독 스모크(`../ai`)를 먼저 통과시킨다
- AWS·Kafka 없이 테스트할 수 있게 Fake 구현을 둔다

### Git

- 커밋: `<type>: <설명>` — **type 앞에 이모지가 붙는다.** `📍 Feat` `🔨 Fix` `📝 Docs` `🎨 Style` `🤖 Refactor` `✅ Test` `🚚 Chore` `✂️ Remove` `🔧 Rename`
- 브랜치: `<type>/<domain>` 소문자 kebab-case
- 원격: `upstream` = 팀 저장소 `github.com/SWM-TeamBruteForce/runiverse-ai-server`, `origin` = 개인 포크 `github.com/jihwanjo-98/runiverse-ai-server`. **둘 다 공개**
- 작업 흐름: 포크(`origin`)에 브랜치 push → 팀 저장소 `dev`로 PR (`--head jihwanjo-98:<브랜치>`). **팀 원격(`upstream`)에는 `main`·`dev`만** 둔다 (기능 브랜치 push 금지)
- 브랜치 역할: `main` 운영 · `dev` 개발 통합. 작업은 기능 브랜치 → PR → `dev`. **PR base는 `dev`** (`main`으로 직접 PR·push 하지 않음)
- push·PR 생성은 요청받았을 때만 한다. 병합은 개발자가 한다
- **push·PR 전 보안 점검 필수**: 저장소 공개 여부 확인 → 올라갈 커밋 범위에서 비밀값·인프라 값(IP·버킷·ARN·계정 ID·EC2·역할 이름) 검색 → 원격 브랜치·PR 상태 확인. 문제가 있으면 push하지 않는다
- 강제 push는 하지 않는다. 불가피하면 원격 브랜치가 병합·변경됐는지 먼저 확인하고 승인받는다
- 한 커밋에 논리적 변경 하나. 각 커밋에서 테스트가 통과하는지 본다
- **AI를 공동 작성자로 넣지 않는다.** `Co-Authored-By: Claude` 트레일러, `🤖 Generated with` 푸터 모두 금지

### Do

- 구현 전에 전제를 말한다. 해석이 갈리면 선택지를 제시하고, 불분명하면 멈추고 묻는다
- 외부 의존(Kafka·S3·SageMaker·DB)은 Fake 구현과 함께 만든다
- 설정값은 시작 시 검증한다. 잘못된 값이면 **시작 시 실패**
- 로그는 단계마다 JSON 한 줄. `inference_id`·`record_card_id`·`stage`를 키로 둔다
- 문서는 개조식 핵심 위주. 서술형 문단 대신 불릿·표
- 모르면 모른다고 한다. SageMaker 응답·결과 파일에 없는 필드를 만들어내지 않는다

### Don't

- 요청 없이 이 파일 수정 — 새 규칙이 필요하면 수정안만 제안한다
- 요청하지 않은 기능·추상화·설정 추가, 안 고장난 것 리팩터링
- 입력 JSON 수정·필드 추가 (컨테이너가 400으로 거부)
- 추출 필드 외의 결과 가공, 음악 파일 검증 (백엔드 몫)
- 자동 재시도
- **회고 원문·사진 캡션·입력 JSON 본문을 로그·로컬 파일에 남기기** (개인정보). 원본 보관은 AI DB `inference_request.input_json`에만 한다
- `KAFKA_PASSWORD`·`DB_PASSWORD`·AWS 키·ARN·버킷 이름을 코드에 하드코딩, 시크릿 커밋
- **공개 저장소다.** 내부 IP·버킷 이름·EC2·IAM 역할 이름·계정 ID를 커밋하지 않는다. 문서·예시에는 `<DB_SERVER_IP>` 같은 자리 표시를 쓰고, 실제 값은 `CLAUDE.local.md`(git 제외)에만 둔다
- test 서버 AI 서버가 실행 중일 때 로컬에서 `KAFKA_ENV=test`로 서버 실행
- AWS CLI·boto3·스크립트로 AWS 리소스를 생성·변경·삭제 — Console 안내로 대신한다
- 검증하지 않은 코드를 "완성"이라고 보고

## Docs

| 주제 | 경로 |
|---|---|
| **구현 계획 (범위·흐름·미결 사항)** | `docs/AI_서버_구현_계획.md` |
| **아키텍처 설계 (구조·DB·작업 순서)** | `docs/AI_서버_아키텍처_설계.md` |
| **입출력 계약 (정본)** | Notion「AI 기능 아키텍처 및 data 입출력」3·4장 |
| 요청 흐름·서버별 책임·Kafka 접속 | Notion「AI 음악 생성 요청 파이프라인」 |
| 버킷·S3 경로·보관 기준 | Notion「S3 버킷 저장 구조」 |
| 컨테이너 입력 검증 | `../ai/src/runiverse_music/schemas.py` |

**저장소 밖**: 음악 생성 컨테이너는 `../ai`, 앱 전체 기획은 `../Docs/`, 앱 코드는 `../frontend/`.

**미확정 정책**(무응답 타임아웃, 운영 배포 위치, 운영 값, 메시지 버전 필드)은 계획 문서 9장에 있다. 가정값으로 구현할 때는 그 사실을 PR에 적는다.
