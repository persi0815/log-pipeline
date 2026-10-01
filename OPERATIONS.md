# Kafka → Logstash → Elasticsearch 실습

프로젝트의 `kafka/`의 3개 브로커와 단일 Elasticsearch 클러스터(3개 노드)를 연결합니다.
전체 서비스는 루트 `compose.yaml`의 단일 `log-pipeline` 프로젝트로 운영합니다.
`produce.py`가 테스트 로그를 생성해 Kafka에 직접 보내는 producer이며 Filebeat는 사용하지 않습니다.

```text
producer/produce.py (가상 HTTP 요청 JSON)
    │ Kafka 공식 console producer, acks=all, 동기 전송
    ▼
Kafka: prac-lab-logs (3 partitions / 3 replicas / min ISR 2)
    │ consumer group: prac-lab-logstash
    ▼
Logstash: main.conf (공백·레벨·시간·숫자만 정제)
    ▼
Elasticsearch: prac-lab-logs-YYYY.MM.dd
    │ 실제 적재 시각 event.ingested / 전체 지연 lab.latency_ms
    ▼
Kibana: PRAC · Kafka → Logstash → Elasticsearch

Metricbeat ── Kafka offsets/lag ──────────────▶ metricbeat-*
           └─ ES / Logstash / Kibana 상태 ───▶ .monitoring-*-8-mb
                                                  │
                                                  ▼
                                       Kibana Stack Monitoring
```

## 바로 보기

- [Kafka UI](http://localhost:8080): 브로커·토픽·메시지·consumer lag 확인 (로그인 없이 로컬 관리 UI)
- [파이프라인 대시보드](http://localhost:5610/app/dashboards#/view/prac-log-pipeline)
- [정제된 로그 원문 / Discover](http://localhost:5610/app/discover#/?_a=(index:prac-lab-logs))
- [Stack Monitoring](http://localhost:5610/app/monitoring): 클러스터 선택 → Logstash → Pipelines → `main`
- 로그인은 기존 `elastic` 계정과 `.env`의 비밀번호를 사용합니다.

대시보드는 10초마다 새로고침합니다. 로그 건수, ERROR 수, 평균 전체 처리 지연, 시간별 레벨, 서비스별 건수,
Kafka 파티션별 소비 지연, 최근 로그가 표시됩니다. 로그 발생이 끝난 뒤 시간이 지나면 시간 범위를 늘려 주세요.
Kafka lag 패널은 **선택한 시간 범위의 파티션별 최댓값**이며 현재값과 다릅니다. 현재 lag는 `status`로 확인합니다.

## 다시 실행하기

Docker Desktop과 Python 3만 있으면 됩니다. Python 외부 패키지는 필요 없습니다.

```bash
cd /Users/persi/Documents/prac/log-pipeline
bash scripts/pipeline.sh up
bash scripts/pipeline.sh produce --count 300 --rate 5
bash scripts/pipeline.sh verify
```

`up`은 단일 Compose로 전체 서비스를 시작하고 인증서·토픽·ES 템플릿·대시보드도 초기화합니다.
기존 클러스터 인증서와 `.env`를 그대로 사용합니다. 네트워크 이름은 `log-pipeline_default`입니다.
`scripts/setup.py`를 다시 실행하면 `prac-lab-*` 이름으로 만든 대시보드·템플릿을 갱신합니다.
Stack Monitoring의 암호화 키 누락 오류를 해결하기 위해 기본 Compose의 Kibana에
`XPACK_ENCRYPTEDSAVEDOBJECTS_ENCRYPTIONKEY`를 연결했고, 키는 기존 `.env`의 `KIBANA_SAVED_OBJECTS_KEY`에 보관합니다.

10분간 초당 2건을 보내며 화면을 관찰하려면:

```bash
bash scripts/pipeline.sh produce --count 1200 --rate 2
```

## 확인 명령

```bash
bash scripts/pipeline.sh status       # Logstash/Metricbeat/Kafka UI 상태 + 파티션별 CURRENT-OFFSET / LOG-END-OFFSET / LAG
bash scripts/pipeline.sh logs         # 처리 로그 보기, Ctrl+C로 종료
bash scripts/pipeline.sh verify       # 마지막 실행의 전송 수 = ES 문서 수, 정제 결과, lag 0, 빈 Logstash 큐 확인
bash scripts/pipeline.sh stop         # 전체 실습 서비스 중지
```

Kafka만 중지하려면 `docker compose stop kafka-1 kafka-2 kafka-3`을 사용합니다.
Kafka에는 기존 `test-topic`도 있으므로, 다른 실습에서 사용 중인지 먼저 확인하세요.
Kafka는 `/tmp/kafka-logs`를 브로커별 external named volume에 연결합니다.
로그와 KRaft 메타데이터, consumer offset을 함께 보존합니다. 이미지 digest도 고정되어 있습니다.
Kafka 볼륨은 external로 관리해 `down -v`에도 유지됩니다. 명시적인 volume 삭제는 데이터를 지웁니다.
마이그레이션 전 원본 데이터 백업은 `results/kafka-backup-*`에 있습니다.

`results/last-run.json`에 실행 ID, `results/raw-events.ndjson`에 producer 원본,
`results/verification.json`에 검증 결과, `results/kafka-lag.txt`에 오프셋이 남습니다.
다음 실행 시 이 파일들은 최신 결과로 바뀌며, Elasticsearch의 이전 로그는 유지됩니다.

## Kafka UI 사용

요청한 [Provectus Kafka UI](https://github.com/provectus/kafka-ui)의 `v0.7.2` 이미지를 사용합니다.
`http://localhost:8080` → `prac-kafka` 클러스터에서 다음을 확인할 수 있습니다.

- **Brokers**: Kafka 브로커 3개와 파티션 배치.
- **Topics → prac-lab-logs → Overview**: 파티션 3개, 복제 계수 3, 파티션 리더와 ISR.
- **Topics → prac-lab-logs → Messages**: Logstash 정제 전의 원본 JSON 로그.
- **Consumers → prac-lab-logstash**: Logstash 소비자 상태, 커밋한 오프셋과 파티션별 lag.

UI는 `127.0.0.1:8080`에만 바인딩하고 토픽 관리 작업을 허용하도록 설정했습니다.
기존 `test-topic`과 Kafka 내부 토픽 `__consumer_offsets`도 함께 표시되므로 전체 토픽 수는 3개입니다.
JMX는 연결하지 않았습니다. UI의 Production/Consumption 바이트 속도 및 자동 감지 버전은
실제 브로커 지표를 나타내지 않을 수 있습니다. 이번 구성에서는 토픽·메시지·오프셋·lag를 확인하세요.

Kafka UI만 시작하거나 중지하려면 프로젝트 루트에서 실행합니다.

```bash
docker compose --env-file .env -f compose.yaml up -d kafka-ui
docker compose --env-file .env -f compose.yaml stop kafka-ui
```

## 정제 내용

| 원본 | Elasticsearch 저장 결과 |
|---|---|
| `"level": " INFO "` | `log.level: "info"` |
| `"message": "  demo request ...  "` | 앞뒤 공백 제거 |
| `"timestamp": "..."` | `@timestamp` 날짜 필드 |
| `"status": "200"` | `http.response.status_code: 200` 정수 |
| `"duration_ms": "57"` | `duration_ms: 57.0` 숫자 |
| `run_id`, `sequence`, `event_id` | `lab.run_id`, `lab.sequence`, `event.id` |

각 10건 중 8건 INFO/200, 1건 WARN/404, 1건 ERROR/500을 생성합니다.
300건이면 INFO 240건, WARN 30건, ERROR 30건입니다. 테스트 데이터만 발생시키며 실제 서비스 요청은 하지 않습니다.

Elasticsearch `_id`에 이벤트 ID를 사용해 같은 이벤트의 재전달이 중복 문서로 쌓이지 않게 했습니다.
Kafka offset은 Logstash 큐에 들어간 시점에 커밋되므로 lag=0만으로 ES 적재 완료를 판단하지 않습니다.
`verify`는 ES 건수와 Logstash persistent queue까지 함께 확인합니다.
Logstash 자체 in/out 카운터는 프로세스 시작 이후 값이므로, 재시작하면 0부터 다시 시작합니다.

## Kafka에 쌓였다가 소비되는 모습 보기

```bash
docker compose --env-file .env -f compose.yaml stop logstash
bash scripts/pipeline.sh produce --count 90 --rate 3
bash scripts/pipeline.sh status  # lag 증가
docker compose --env-file .env -f compose.yaml start logstash
bash scripts/pipeline.sh verify  # 모두 ES로 이동, lag 0
```

## 자원과 범위

- Logstash: heap 256MB, 컨테이너 768MB, worker 1, persistent queue 최대 64MB.
- Metricbeat: 컨테이너 최대 256MB, Kafka/Logstash 지표 10초, ES/Kibana 지표 20초.
- Kafka UI: JVM heap 최대 256MB, 컨테이너 최대 512MB.
- Kafka 테스트 토픽은 24시간 보존입니다. Elasticsearch 로그는 자동 삭제하지 않습니다.
- 로그 생성은 기본 300건으로 종료됩니다. 모니터링은 `stop` 전까지 계속 수집합니다.
- 같은 Elasticsearch에 모니터링 데이터를 넣는 로컬 학습 구성입니다. 클러스터가 내려가면 모니터링 저장도 중단됩니다.
- 연결은 기존 CA로 TLS 인증서를 검증합니다. 편의를 위해 기존 elastic 계정을 사용하며 운영용 권한 구성은 아닙니다.
- 다른 대규모 실습 환경과 함께 실행하면 Docker 메모리가 부족해질 수 있습니다.

## 참고

- [Kafka input 설정](https://www.elastic.co/docs/reference/logstash/plugins/plugins-inputs-kafka/)
- [Metricbeat Kafka 지표](https://www.elastic.co/docs/reference/beats/metricbeat/metricbeat-module-kafka)
- [Metricbeat를 통한 Logstash Stack Monitoring](https://www.elastic.co/docs/reference/logstash/monitoring-with-metricbeat)

## 최초 실행 확인 결과 (2026-09-29)

- 최종 실행 ID: `20260929T055920Z-9ff34ff2`
- 전송 300건 / Elasticsearch 적재 300건: INFO 240, WARN 30, ERROR 30
- 평균 전체 지연 96.2ms, 최대 914ms
- Kafka 3개 파티션의 현재 lag 0, Logstash 큐 0
- 앞선 배치 대기 진단 실행도 300건 적재되어, 초기 대시보드에는 총 600건이 표시됩니다.
  첫 실행은 producer 기본 배치 대기 1초 때문에 느렸으며 `--timeout 0`으로 수정했습니다.
  두 실행을 합친 평균 지연은 최종 실행의 평균 96.2ms보다 크게 보입니다.

## 배포 단위

역할별 파일은 루트 `compose.yaml`에서 `include`합니다. 개별 파일을 단독 실행하지 마세요.
전체 서비스는 하나의 `log-pipeline` 프로젝트와 기본 네트워크에서 실행하며 컨테이너는 각각 독립적입니다.

```bash
docker compose up -d --wait --wait-timeout 600
docker compose ps -a
docker compose logs -f logstash
docker compose stop
```

`certgen`, `setup`, `kafka-init`, `lab-init`은 초기화 후 `Exited (0)`으로 종료됩니다.
기존 데이터 볼륨은 external로 연결해 재사용합니다. 최초 볼륨 생성 방법은 README를 참고하세요.
Kafka뿐 아니라 ES·Kibana·Logstash·Metricbeat 볼륨도 Compose의 `down -v`로 삭제되지 않습니다.
단일 호스트 장애에 대한 이중화는 제공하지 않습니다.
