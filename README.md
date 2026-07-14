# Filter Enrichment Service

Sits in front of the Delivery Engine and does **pre-filtering + enrichment** of source objects. It
reads a Kafka topic, decides which active subscriptions an object is potentially relevant to,
enriches the object (or a before/after pair) via the **Object Enrich Service**, applies the full
filters on the enriched data, and publishes matched objects — together with the list of matched
subscriptions — to the shared **Enriched Objects** topic consumed by the Delivery Engine.

```
Source Objects Topic ─▶ Filter Enrichment Service ─▶ Object Enrich Service
                                      │
                                      ▼
                          Enriched Objects Topic ─▶ Delivery Engine ─▶ Consumer Topics
```

This is a standalone application and its own Git repository (separate from Subscription Service and
Delivery Engine), with its own Docker image, Helm chart, CI and release lifecycle.

## Pipeline

1. **Consume** `objects.flat` (all versions of an object share a partition via the object key).
2. **Parse & validate** the record (real source format — see below). The type is inferred from
   structure: a body with `before` and `after` objects is `BEFORE_AFTER`, a bare object is `OBJECT`.
   Structurally invalid records → **input DLQ**.
3. **Pre-match** candidate subscriptions on the *flat* payload using three-valued (Kleene) logic: a
   comparison over a field present in the flat payload is decided; one that needs enrichment is
   `UNKNOWN`. A subscription is excluded only when its pre-match is definitively `FALSE`
   (`BEFORE_AFTER`: only when *both* before and after are false). No candidates → drop.
4. **Union required fields** across candidates (`outputField` list = subscription fields + filter
   fields, de-duplicated, deterministically sorted).
5. **Enrich once** — one HTTP call per record, no micro-batching:
   - `OBJECT`: `GET /api/v1/enriched-objects/{objectClass}?globalId=..&outputField=..`
   - `BEFORE_AFTER`: `POST /api/v1/enriched-objects/{objectClass}/revisions?outputField=..` with a
     body of both version ids (`before.id`, `after.id`); the response is matched back **by `id`**
     (order-independent, both required, no duplicates).
6. **Filter** the enriched object(s). `BEFORE_AFTER` reports `beforeMatched` / `afterMatched` per
   subscription; a subscription is included if either is true. A filter is never treated as false
   just because a field is missing (§28): if it cannot be computed the message goes to the
   **enrichment DLQ**.
7. **Publish** at most one output record (§21/§22) keyed by `objectId`, or drop if nothing matched.

## Input format

The source producer sends the flat object itself — no envelope, no `messageType`, no `payload`
wrapper. A `BEFORE_AFTER` change is `{ "before": {…}, "after": {…} }`; everything else is a single
`OBJECT`. Field mapping into the internal model:

| Internal | Source field | Notes |
|----------|--------------|-------|
| `messageType` | *(structure)* | `before`+`after` ⇒ BEFORE_AFTER, else OBJECT |
| `objectClass` | `objectClass` → `objectType` | reads `objectClass`, falls back to `objectType` during the rename |
| `objectId` (Kafka key / output) | `globalId` | stable across revisions |
| `revisionId` (version id, `/revisions` body) | `id` | unique per version; before/after are distinguished **only** by their tags |
| `sourceEventId` | `revisionEventId` | of the current (`after`) version; must be stable for idempotency (§30) |
| `payload` | *the whole flat object* | fields are top-level (`portfolioId`, `status`, …) |

## Metadata & filter compilation

The domain model is loaded once from `GET /api/config/domain` at startup (and on config reload) and
kept **in memory** — never fetched per message. RSQL filters are compiled once (startup / on
`CONFIG_CHANGED` / after metadata reload) into a full predicate + a tri-state pre-filter. A filter
that cannot be compiled (unknown field, traverses a to-many collection) fails the subscription via
`POST /internal/subscriptions/{id}/fail` and is not added to the local registry.

## Runtime subscriptions (Redis)

- Full set: `subs:runtime`; config: `sub:{subscriptionId}`.
- Only `status == ACTIVE` and the served engine (`OBJECT_BATCH` by default) are used.
- Pub/Sub channel `subscriptions:changes` carries `{"type":"CONFIG_CHANGED","subscriptionId":...}`
  as a **signal**; the service re-reads that subscription (or does a full reload when no id is given).
- Postgres is never used.

## Reliability

- **At-least-once** (§30): the input offset is committed only after the record is published, dropped
  (no candidates / no matches) or written to a DLQ. If even the DLQ write fails, the record is
  redelivered.
- **Retry**: exponential backoff + jitter on retryable enrich errors (429/502/503/504, timeouts,
  connection reset). `400` / invalid outputField / malformed → non-retryable; `404` → not found.
  After retries a failed enrich → enrichment DLQ.
- **Circuit breaker + bulkhead** on the Enrich client (resilience4j); pooled keep-alive HTTP.
- **Backpressure** (§31): a bounded permit pool caps in-flight enrich requests; when saturated the
  consumer pauses its partitions until capacity frees up (`filter_enrichment_paused_partitions`).
- **DLQs**: `filter-enrichment.input.dlq`, `filter-enrichment.enrichment.dlq`,
  `filter-enrichment.output.dlq`.

## Endpoints

- `GET /health/live`, `GET /health/ready` — probes (readiness requires Redis, subscriptions,
  metadata, Kafka consumer/producer and the enrich circuit breaker not being OPEN).
- `GET /actuator/health`, `GET /actuator/prometheus`, `GET /actuator/metrics`.

Metrics are prefixed `filter_enrichment_*` (§35); `subscriptionId` is never used as a label.

## Configuration

Everything is externalized (`filter-enrichment.*`), each key overridable via an env var — see
`src/main/resources/application.yml`. Key ones:

| Env | Default | Meaning |
|-----|---------|---------|
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka brokers |
| `INPUT_TOPIC` / `OUTPUT_TOPIC` | `objects.flat` / `objects.enriched` | topics |
| `REDIS_HOST` / `REDIS_PORT` | `localhost` / `6379` | Redis |
| `ENRICH_SERVICE_URL` | `http://object-enrich-service:8080` | Object Enrich Service |
| `SUBSCRIPTION_SERVICE_URL` | `http://subscription-service:8080` | fail API |
| `SERVED_ENGINE` | `OBJECT_BATCH` | engine filter |
| `MAX_CONCURRENT_HTTP` | `32` | backpressure / bulkhead bound |

## Build, test, run

```bash
mvn verify                 # build + unit tests
docker build -t filter-enrichment-service:0.1.0 .
helm install fes helm/filter-enrichment-service
```

## Tests

Unit tests cover the tri-state pre-match & Kleene logic, filter compilation (collection rejection),
required-fields union, revision matching (array vs object shapes, missing/duplicate), and the
`OBJECT` / `BEFORE_AFTER` processing flows (drop-no-candidates, publish with `matchedSubscriptionIds`,
`beforeMatched || afterMatched`, enrichment-DLQ on failure/missing revision).

Integration (embedded Kafka + Redis + a mock Enrich Service) and contract tests against the real
`/api/config/domain`, `GET .../{objectClass}` and `POST .../revisions` are the recommended next
layer — the DTO parsers are deliberately lenient (`@JsonIgnoreProperties`) and `RevisionMatcher`
tolerates both plausible `/revisions` response shapes (§37).

## Design notes / assumptions

- `GET /api/config/domain` is modeled on the search-service metadata shape (classes / declared
  scalar fields / hierarchy / relations); the exact contract should be pinned by a contract test.
- The enriched payload is deep-merged over the flat source payload so the output carries both.
- Partial enrichment (§28): user-only missing fields are published with `enrichmentStatus=PARTIAL`
  and a `missingFields` list; a missing **filter** field makes the filter uncomputable → the whole
  message is routed to the enrichment DLQ (conservative — never silently treats the filter as false).
- Out of scope (V1): initialization messages, micro-batching, per-subscriber projection/topics,
  membership, Postgres, Kafka topic/ACL management, exactly-once.
