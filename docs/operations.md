# Эксплуатация и runbook

## Health-пробы

| Проба | Endpoint | Ready когда |
|---|---|---|
| Liveness | `GET /health/live` | Процесс поднят (`livenessState`) |
| Readiness | `GET /health/ready` | Все контрибьюторы UP: `redis`, `subscriptions`, `metadata`, `kafkaConsumer`, `kafkaProducer`, `enrichService` |

Пробы отдают `200` при `UP` и `503` иначе (`HealthProbeController`). Дублируются стандартными
`GET /actuator/health`, `GET /actuator/prometheus`, `GET /actuator/metrics`.

### Контрибьюторы readiness

| Контрибьютор | UP когда | Класс |
|---|---|---|
| `metadata` | доменная модель загружена из DataDictionary | `MetadataHealthIndicator` |
| `subscriptions` | runtime-подписки загружены из Redis | `SubscriptionsHealthIndicator` |
| `kafkaConsumer` | listener-контейнеры запущены | `KafkaConsumerHealthIndicator` |
| `kafkaProducer` | кластер Kafka достижим (`describeCluster`) | `KafkaProducerHealthIndicator` |
| `enrichService` | circuit breaker обогащения не `OPEN`/`FORCED_OPEN` (`HALF_OPEN` = UP) | `EnrichServiceHealthIndicator` |
| `redis` | Redis достижим | стандартный Spring Boot indicator |

Поскольку consumer’ы стартуют только после загрузки метамодели и подписок, то, что под какое-то время
на старте **live, но не ready**, — нормально: он ждёт DataDictionary / Redis. Если ready так и не
наступает — в первую очередь проверьте эти зависимости и детали `/health/ready`.

## Метрики (Micrometer / Prometheus)

Отдаются на `/actuator/prometheus`. Все с префиксом `filter_enrichment_`. `subscriptionId` в лейблах
**не** используется (неограниченная кардинальность).

| Метрика | Тип | Смысл |
|---|---|---|
| `filter_enrichment_input_total` | counter | Записи, прочитанные из `objects.flat` |
| `filter_enrichment_input_by_type_total{type=OBJECT\|BEFORE_AFTER}` | counter | Записи по выведенному типу |
| `filter_enrichment_dropped_no_candidates_total` | counter | Отброшено на предматче (нет кандидатов) |
| `filter_enrichment_dropped_no_matches_total` | counter | Отброшено после фильтра (нет совпадений) |
| `filter_enrichment_candidate_subscriptions_total` | summary | Распределение числа кандидатов на запись |
| `filter_enrichment_matched_subscriptions_total` | summary | Распределение числа совпадений на запись |
| `filter_enrichment_get_requests_total` | counter | Запросы `GET .../{objectClass}` (OBJECT) |
| `filter_enrichment_revision_requests_total` | counter | Запросы `POST .../revisions` (BEFORE_AFTER) |
| `filter_enrichment_http_requests_total{outcome=success\|error}` | counter | Исходы HTTP-вызовов Enrich Service |
| `filter_enrichment_http_errors_total` | counter | Ошибки обогащения (по попыткам) |
| `filter_enrichment_http_latency_seconds` | timer | Латентность вызова Enrich Service |
| `filter_enrichment_retry_total` | counter | Ретраи обогащения |
| `filter_enrichment_partial_total` | counter | Публикации со статусом `PARTIAL` |
| `filter_enrichment_output_total` | counter | Опубликованные конверты в `objects.enriched` |
| `filter_enrichment_dlq_total{dlq=input\|enrichment\|output}` | counter | Записи в каждую DLQ |
| `filter_enrichment_config_reload_total` | counter | Перезагрузки конфигурации подписок |
| `filter_enrichment_active_subscriptions` | gauge | Обслуживаемые подписки в registry этого пода |
| `filter_enrichment_in_flight_requests` | gauge | Enrich-запросы «в полёте» |
| `filter_enrichment_paused_partitions` | gauge | Партиции на паузе (backpressure) |

### Предлагаемые алерты

- `rate(filter_enrichment_dlq_total[5m]) > 0` — записи уходят в DLQ; разобраться по `dlq`-лейблу.
- readiness лежит > нескольких минут — отказ зависимости (Kafka / Redis / DataDictionary / Enrich).
- `filter_enrichment_active_subscriptions == 0`, когда подписки ожидаются — проблема загрузки из Redis.
- устойчиво ненулевой `filter_enrichment_paused_partitions` — постоянный backpressure (Enrich не
  успевает; смотрите латентность и circuit breaker).
- устойчиво высокий `rate(filter_enrichment_retry_total[5m])` — давление на Enrich Service.

## Dead-letter-очереди

Три DLQ разделяют классы ошибок. Каждая DLQ-запись сохраняет исходные key/value и добавляет заголовок
`error-reason`.

| DLQ | Топик | Кем наполняется | Типичный `error-reason` |
|---|---|---|---|
| input | `filter-enrichment.input.dlq` | Битый/нераспознанный вход | `invalid JSON: …`, `message is not a JSON object`, `BEFORE_AFTER requires both before and after objects`, `missing objectClass/objectType`, `missing/invalid globalId`, `missing/invalid id` |
| enrichment | `filter-enrichment.enrichment.dlq` | Обогащение упало / ревизия не сопоставилась / поле фильтра недоступно | `enrich object failed: …`, `enrich revisions failed: …`, `missing enriched revision {rev}`, `duplicate enriched revision {rev}`, `filter field missing after enrichment for {subscriptionId}` |
| output | `filter-enrichment.output.dlq` | Сериализация/публикация не удались после ретраев | `serialization error: …`, `publish failed: …` |

Посмотреть можно любым consumer’ом:

```bash
kafka-console-consumer --bootstrap-server $KAFKA --topic filter-enrichment.enrichment.dlq \
  --from-beginning --property print.headers=true
```

**Переобработка:**

- **input** — проблемы данных выше по потоку (дрейф формата, отсутствующие поля). Почините продюсера
  источника, затем при желании переиграйте.
- **enrichment** — либо транзиентная недоступность Enrich Service (после восстановления можно
  переиграть исходные записи), либо систематическая проблема данных/полноты (незаполненные поля
  фильтра). Смотрите `error-reason` и полноту обогащения в Enrich Service.
- **output** — обычно транзиентные проблемы брокера/топика; после восстановления payload можно
  переопубликовать (ключ `objectId` сохранён в ключе записи).

Если даже запись в DLQ падает, offset не коммитится и запись переигрывается — потери нет.

## Backpressure

`BackpressureManager` держит семафор на `MAX_CONCURRENT_HTTP` одновременных enrich-запросов. Если
разрешение не удаётся получить за `BACKPRESSURE_ACQUIRE_TIMEOUT_MS`, партиции consumer ставятся на
паузу (опрос прекращается) до освобождения ёмкости, затем возобновляются. Тот же лимит дублируется
bulkhead’ом resilience4j; при его насыщении (`BackpressureException`) запись не фейлится и не DLQ’ится
— она переобрабатывается под backpressure. Наблюдать: `filter_enrichment_paused_partitions`,
`filter_enrichment_in_flight_requests`.

## Масштабирование

- Stateless; масштабируется числом реплик в одной consumer group.
- Полезный потолок параллелизма = число партиций `objects.flat`. Поды сверх этого простаивают
  (в Helm `values.yaml`: держите `replicaCount <= partitions`).
- Все ревизии объекта делят входной ключ (`globalId`), поэтому порядок в рамках объекта сохраняется
  при любом числе подов — и на входе, и на выходе (`objects.enriched` тоже кейится по `objectId`).

## Runbook — частые симптомы

| Симптом | Вероятная причина | Что проверить |
|---|---|---|
| Под не становится ready | DataDictionary / Redis / Kafka / Enrich недоступны | детали `/health/ready`; связность метамодели, Redis, брокера; состояние circuit breaker |
| Delivery-движки вообще не видят объектов | Нет активных подписок в Redis | `filter_enrichment_active_subscriptions`; что подписки со `status = ACTIVE` есть в `subs:runtime` (тип движка тут не важен — обслуживаются все) |
| Всё уходит в drop (нет выхода) | Предматч отбраковывает всё либо фильтр не совпадает | `filter_enrichment_dropped_no_candidates_total` vs `…dropped_no_matches_total`; таргеты и фильтры подписок |
| Конкретная подписка ничего не даёт | Не обслуживается здесь или FAILED (некомпилируемый фильтр) | `sub:{id}` в Redis; логи `Filter compilation failed` / отчёт FAILED в Subscription Service |
| Растёт input DLQ | Дрейф формата источника | заголовок `error-reason` на `…input.dlq` |
| Растёт enrichment DLQ | Enrich Service падает или поля фильтра не заполняются | `error-reason`; латентность/ошибки обогащения; circuit breaker; полнота данных в Enrich Service |
| Растёт output DLQ | Проблема брокера / выходного топика | health брокера; топик `objects.enriched` доступен на запись; ретраи |
| Много `PARTIAL` | Проекционные поля не заполняются обогащением | `metadata.missingFields` в выходе; данные в Enrich Service |
| Постоянно ненулевой `paused_partitions` | Enrich Service не успевает | латентность обогащения; `MAX_CONCURRENT_HTTP`; здоровье Enrich Service |
| Circuit breaker `enrichService` DOWN | Enrich Service недоступен/деградирует | доступность Enrich Service; после восстановления breaker перейдёт HALF_OPEN → CLOSED |

## Деплой

Самостоятельный Spring Boot-сервис. Сборка и запуск:

```bash
mvn clean package
java -jar target/filter-enrichment-service-0.1.0.jar
```

Docker и Helm:

```bash
docker build -t filter-enrichment-service:0.1.0 .
helm install fes helm/filter-enrichment-service
```

Конфигурация полностью через env — см. [конфигурацию](configuration.md). Helm-деплой задаёт
liveness/readiness пробы (`/health/live`, `/health/ready`) и Prometheus-аннотации на под. Сервис имеет
собственный цикл Docker image и Helm chart (отдельно от Subscription Service и Delivery Engine).

См. также: [контракт](contract.md) · [архитектура](architecture.md) · [конфигурация](configuration.md)
