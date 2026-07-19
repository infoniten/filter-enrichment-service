# Конфигурация

Все настройки — в `application.yml` под префиксом `filter-enrichment.*` (плюс стандартные `spring.*`
и `management.*`). Каждый ключ переопределяется переменной окружения. Дефолты
производственно-разумные; в таблицах указаны свойство, env-override (где есть) и значение по умолчанию
строго из кода.

## Подключения Kafka и Redis

| Свойство | Env | По умолчанию |
|---|---|---|
| `spring.kafka.bootstrap-servers` | `KAFKA_BOOTSTRAP` | `localhost:9092` |
| `spring.data.redis.host` | `REDIS_HOST` | `localhost` |
| `spring.data.redis.port` | `REDIS_PORT` | `6379` |
| `spring.data.redis.timeout` | — | `2s` |

## Тип обслуживаемого движка

| Свойство | Env | По умолчанию | Примечания |
|---|---|---|---|
| `filter-enrichment.served-engine` | `SERVED_ENGINE` | `OBJECT_BATCH` | Обслуживаются только подписки `status = ACTIVE` **и** `engine = SERVED_ENGINE` |

## Kafka — топики, consumer group, DLQ

| Свойство | Env | По умолчанию | Примечания |
|---|---|---|---|
| `filter-enrichment.kafka.input-topic` | `INPUT_TOPIC` | `objects.flat` | Топик исходных объектов |
| `filter-enrichment.kafka.output-topic` | `OUTPUT_TOPIC` | `objects.enriched` | Общий топик обогащённых объектов (вход Delivery-движков) |
| `filter-enrichment.kafka.consumer-group` | `CONSUMER_GROUP` | `filter-enrichment-service` | Consumer group сервиса |
| `filter-enrichment.kafka.concurrency` | `KAFKA_CONCURRENCY` | `3` | Concurrency listener-контейнера; полезный потолок — число партиций входного топика |
| `filter-enrichment.kafka.dlq.input` | `DLQ_INPUT_TOPIC` | `filter-enrichment.input.dlq` | Битые/нераспознанные входные записи |
| `filter-enrichment.kafka.dlq.enrichment` | `DLQ_ENRICHMENT_TOPIC` | `filter-enrichment.enrichment.dlq` | Ошибки обогащения / невычислимый фильтр |
| `filter-enrichment.kafka.dlq.output` | `DLQ_OUTPUT_TOPIC` | `filter-enrichment.output.dlq` | Сериализация/публикация не удались после ретраев |

Producer и consumer настроены в коде (`KafkaConfig`): продюсер — `acks=all`, идемпотентность,
сжатие `zstd`, `linger.ms=20`, `batch.size=65536` (только транспортный batching); consumer —
без авто-коммита, `AckMode.RECORD`, `auto.offset.reset=earliest`, `max.poll.records=100`,
`autoStartup=false` (старт после загрузки метамодели + подписок).

## Redis — ключи контракта подписок

| Свойство | Env | По умолчанию | Примечания |
|---|---|---|---|
| `filter-enrichment.redis.channel` | `REDIS_CHANNEL` | `subscriptions:changes` | Pub/sub-канал для `CONFIG_CHANGED` |
| `filter-enrichment.redis.runtime-set-key` | `REDIS_RUNTIME_SET_KEY` | `subs:runtime` | Множество id runtime-подписок |
| `filter-enrichment.redis.config-key-prefix` | `REDIS_CONFIG_KEY_PREFIX` | `sub:` | Префикс ключей `sub:{id}` |

## Метамодель (DataDictionary)

Доменная модель грузится на старте и при перезагрузках — никогда на каждое сообщение.

| Свойство | Env | По умолчанию |
|---|---|---|
| `filter-enrichment.metamodel.base-url` | `DATA_DICTIONARY_URL` | `http://data-dictionary:8080` |
| `filter-enrichment.metamodel.metadata-path` | `DATA_DICTIONARY_METADATA_PATH` | `/api/search-service/metadata/v3` |
| `filter-enrichment.metamodel.connect-timeout-ms` | `METAMODEL_CONNECT_TIMEOUT_MS` | `2000` |
| `filter-enrichment.metamodel.read-timeout-ms` | `METAMODEL_READ_TIMEOUT_MS` | `5000` |

## Object Enrich Service (HTTP-клиент)

| Свойство | Env | По умолчанию |
|---|---|---|
| `filter-enrichment.enrich.base-url` | `ENRICH_SERVICE_URL` | `http://object-enrich-service:8080` |
| `filter-enrichment.enrich.connect-timeout-ms` | `ENRICH_CONNECT_TIMEOUT_MS` | `2000` |
| `filter-enrichment.enrich.read-timeout-ms` | `ENRICH_READ_TIMEOUT_MS` | `5000` |
| `filter-enrichment.enrich.max-connections` | `ENRICH_MAX_CONNECTIONS` | `100` |
| `filter-enrichment.enrich.max-connections-per-route` | `ENRICH_MAX_CONNECTIONS_PER_ROUTE` | `100` |

### Circuit breaker обогащения (resilience4j)

Трипается только на ошибках здоровья апстрима (RETRYABLE); клиентские 400/404 breaker не трогают.

| Свойство | Env | По умолчанию |
|---|---|---|
| `filter-enrichment.enrich.circuit-breaker.failure-rate-threshold` | `ENRICH_CB_FAILURE_RATE` | `50` (%) |
| `filter-enrichment.enrich.circuit-breaker.wait-duration-in-open-state-ms` | `ENRICH_CB_WAIT_MS` | `10000` |
| `filter-enrichment.enrich.circuit-breaker.sliding-window-size` | `ENRICH_CB_WINDOW` | `50` |
| `filter-enrichment.enrich.circuit-breaker.minimum-number-of-calls` | `ENRICH_CB_MIN_CALLS` | `20` |

## Retry (exponential backoff + jitter)

Используется для обогащения (собственный цикл `EnrichClient`), публикации, DLQ-записи и
FAILED-callback’а (`RetryTemplate`).

| Свойство | Env | По умолчанию |
|---|---|---|
| `filter-enrichment.retry.max-attempts` | `RETRY_MAX_ATTEMPTS` | `4` |
| `filter-enrichment.retry.initial-backoff-ms` | `RETRY_INITIAL_BACKOFF_MS` | `200` |
| `filter-enrichment.retry.max-backoff-ms` | `RETRY_MAX_BACKOFF_MS` | `5000` |
| `filter-enrichment.retry.multiplier` | `RETRY_MULTIPLIER` | `2.0` |
| `filter-enrichment.retry.jitter` | `RETRY_JITTER` | `0.3` |

## Backpressure

Ограничивает число одновременных enrich-запросов, чтобы под не накапливал неограниченную работу. Тот
же лимит — у bulkhead resilience4j.

| Свойство | Env | По умолчанию | Примечания |
|---|---|---|---|
| `filter-enrichment.backpressure.max-concurrent-http-requests` | `MAX_CONCURRENT_HTTP` | `32` | Число одновременных enrich-запросов (семафор + bulkhead) |
| `filter-enrichment.backpressure.acquire-timeout-ms` | `BACKPRESSURE_ACQUIRE_TIMEOUT_MS` | `1000` | Сколько воркер ждёт разрешение до паузы партиций |
| `filter-enrichment.backpressure.pause-ms` | `BACKPRESSURE_PAUSE_MS` | `500` | Длительность паузы партиций (по javadoc свойства) |

## Гейтинг на старте

| Свойство | Env | По умолчанию | Примечания |
|---|---|---|---|
| `filter-enrichment.startup.retry-interval-ms` | `STARTUP_RETRY_INTERVAL_MS` | `5000` | Период повтора загрузки метамодели + подписок до старта consumer’ов |

## Subscription Service (fail API)

| Свойство | Env | По умолчанию | Примечания |
|---|---|---|---|
| `filter-enrichment.subscription-service.base-url` | `SUBSCRIPTION_SERVICE_URL` | `http://subscription-service:8080` | `POST /internal/subscriptions/{id}/fail` при некомпилируемом фильтре |

## Наблюдаемость и health

| Свойство | По умолчанию | Примечания |
|---|---|---|
| `management.endpoints.web.exposure.include` | `health,info,metrics,prometheus` | |
| readiness-группа | `readinessState, redis, subscriptions, metadata, kafkaConsumer, kafkaProducer, enrichService` | Под ready только когда всё поднято |
| liveness-группа | `livenessState` | |
| `management.endpoint.health.show-details` | `always` | |
| `logging.level.com.example.filterenrichment` | `INFO` | Поднимите до `DEBUG`, чтобы трейсить маршрутизацию/пропуски по записям |

## Минимальный пример env для деплоя

```bash
KAFKA_BOOTSTRAP=kafka:9092
REDIS_HOST=redis
REDIS_PORT=6379
INPUT_TOPIC=objects.flat
OUTPUT_TOPIC=objects.enriched
CONSUMER_GROUP=filter-enrichment-service
SERVED_ENGINE=OBJECT_BATCH
DATA_DICTIONARY_URL=http://data-dictionary:8080
ENRICH_SERVICE_URL=http://object-enrich-service:8080
SUBSCRIPTION_SERVICE_URL=http://subscription-service:8080
MAX_CONCURRENT_HTTP=32
```

Helm-chart (`helm/filter-enrichment-service`) прокидывает эти переменные через ConfigMap; см.
`values.yaml` (секция `config`).

См. также: [эксплуатация](operations.md) · [архитектура](architecture.md) · [контракт](contract.md)
