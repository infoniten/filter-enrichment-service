# Контракт

Сервис читает **входной** топик `objects.flat` (реальный формат исходных объектов) и производит
**выходной** топик `objects.enriched` — стабильный публичный интерфейс, который потребляют
Delivery-движки. Любое изменение выходного формата считается breaking.

---

## Входной контракт (`objects.flat`)

Продюсер источника присылает **сам плоский объект** — без конверта, без `messageType`, без обёртки
`payload`. Тип выводится из структуры: тело с объектами `before` и `after` — это `BEFORE_AFTER`;
любое другое тело — одиночный `OBJECT`.

| | |
|---|---|
| **Топик** | `objects.flat` (env `INPUT_TOPIC`) |
| **Ключ** | `globalId` объекта (строка UTF-8) — все версии объекта попадают в одну партицию |
| **Значение** | один плоский JSON-объект (`OBJECT`) или `{ "before": {…}, "after": {…} }` (`BEFORE_AFTER`) |

### Маппинг во внутреннюю модель (`InputMessage`)

| Внутреннее поле | Источник | Примечания |
|---|---|---|
| `messageType` | *(структура)* | `before` + `after` ⇒ `BEFORE_AFTER`, иначе `OBJECT` |
| `objectClass` | `objectClass` → `objectType` | читается `objectClass`, при отсутствии — fallback на `objectType` (на время переименования) |
| `objectId` (ключ Kafka / выход) | `globalId` | стабилен между ревизиями |
| `revisionId` (id версии, тело `/revisions`, ключ дедупа downstream) | `id` | уникален на версию; before/after различаются **только** по своим тегам |
| `savedAt` | `savedAt` | опционально |
| `payload` | *весь плоский объект* | поля на верхнем уровне (`portfolioId`, `status`, …) |

`globalId` и `id` принимаются и как число, и как числовая строка (`"globalId": "110831655"`). Для
`BEFORE_AFTER` `objectClass`/`objectId`/`revisionId` берутся со стороны `after`.

### Валидация входа

Структурно некорректная запись уходит в **input DLQ** (offset при этом коммитится):

- невалидный JSON или не JSON-объект;
- `BEFORE_AFTER` только с одной из сторон (`before` без `after` или наоборот);
- отсутствует `objectClass`/`objectType`;
- отсутствует/некорректен `globalId` или `id`.

### Пример входа

`OBJECT`:
```json
{ "objectClass": "FxSpotForwardTrade", "globalId": 42, "id": 1007,
  "portfolioId": "P1", "status": "LIVE", "savedAt": "2026-07-17T10:00:00Z" }
```

`BEFORE_AFTER`:
```json
{ "before": { "objectClass": "FxSpotForwardTrade", "globalId": 42, "id": 1006, "status": "PENDING" },
  "after":  { "objectClass": "FxSpotForwardTrade", "globalId": 42, "id": 1007, "status": "LIVE" } }
```

---

## Выходной контракт (`objects.enriched`)

| | |
|---|---|
| **Топик** | `objects.enriched` (env `OUTPUT_TOPIC`) — общий вход Delivery-движков |
| **Ключ** | `objectId` (= `globalId`) — все версии объекта в одной партиции и упорядочены |
| **Значение** | один JSON-конверт: `OBJECT` **или** `BEFORE_AFTER` (никогда не массив — на запись максимум одно сообщение) |
| **Заголовки** | на happy path нет (у DLQ-записей есть `error-reason`) |

Форма конверта определяется по наличию полей: `object` ⇒ `OBJECT`, `before`/`after` ⇒ `BEFORE_AFTER`.
Обогащённые объекты **самоописательны** (несут `objectClass`, `globalId`, `id`, `savedAt` и т.д. плюс
запрошенные поля), поэтому кладутся как есть — без обёртки и без дублирования полей в конверте.

### Конверт `OBJECT`

Отправляется для одиночного объекта, совпавшего хотя бы с одной обслуживаемой подпиской.

```json
{
  "matchedSubscriptionIds": ["sub-123", "sub-456"],
  "object": {
    "objectClass": "FxSpotForwardTrade",
    "globalId": 42,
    "id": 1007,
    "portfolioId": "P1",
    "counterparty": { "code": "ACME" }
  },
  "metadata": {
    "enrichmentStatus": "FULL",
    "enrichedAt": "2026-07-17T10:00:01Z"
  }
}
```

### Конверт `BEFORE_AFTER`

Отправляется для пары ревизий, где хотя бы одна подписка совпала на `before` **или** `after`. Несёт по
подписке флаги `beforeMatched` / `afterMatched` — из них Delivery-движки решают, что делать (например,
`delivery-engine-event`: `afterMatched` без `beforeMatched` — новое совпадение, `beforeMatched` без
`afterMatched` — выход из выборки → `REMOVE`).

```json
{
  "subscriptionMatches": [
    { "subscriptionId": "sub-123", "beforeMatched": false, "afterMatched": true },
    { "subscriptionId": "sub-456", "beforeMatched": true,  "afterMatched": false }
  ],
  "before": { "objectClass": "FxSpotForwardTrade", "globalId": 42, "id": 1006, "status": "PENDING" },
  "after":  { "objectClass": "FxSpotForwardTrade", "globalId": 42, "id": 1007, "status": "LIVE" },
  "metadata": {
    "enrichmentStatus": "PARTIAL",
    "enrichedAt": "2026-07-17T10:00:01Z",
    "missingFields": ["FxSpotForwardTrade.counterparty.rating"]
  }
}
```

Комбинация `beforeMatched=false, afterMatched=false` в `subscriptionMatches` не встречается: подписка
попадает в массив, только если совпала хотя бы на одной стороне.

### Справочник полей

| Поле | Тип | Где | Примечания |
|---|---|---|---|
| `matchedSubscriptionIds` | string[] | `OBJECT` | Все обслуживаемые локально подписки, совпавшие с объектом. Всегда ≥ 1 |
| `object` | object | `OBJECT` | Обогащённый объект целиком (как вернул Enrich Service) |
| `subscriptionMatches` | object[] | `BEFORE_AFTER` | По совпавшей подписке: `subscriptionId`, `beforeMatched`, `afterMatched` |
| `before` / `after` | object | `BEFORE_AFTER` | Обогащённые объекты соответствующих ревизий |
| `metadata` | object | оба | Полнота обогащения (см. ниже) |

### Семантика `metadata`

```json
"metadata": { "enrichmentStatus": "FULL" | "PARTIAL", "enrichedAt": "<ISO-8601>", "missingFields": [ … ] }
```

| Поле | Смысл |
|---|---|
| `enrichmentStatus` | `FULL` — все запрошенные `required`-поля заполнены; `PARTIAL` — часть полей обогащение не смогло заполнить |
| `enrichedAt` | момент обогащения (ISO-8601, UTC) |
| `missingFields` | список незаполненных `required`-полей в формате `Class.path.to.field`; присутствует **только** при `PARTIAL` |

`required`-поля — это объединение `fields` подписок и полей, на которые ссылается фильтр (для всех
кандидатов записи), запрошенное у Enrich Service одним набором `outputField`. Поле считается
отсутствующим, если после обогащения по нему нет скалярного значения (null-связь, неразрезолвленная
ссылка или поле, запрошенное другой подпиской этой же записи).

Важно: `PARTIAL` касается только **проекционных** полей. Если незаполненным оказывается поле, нужное
самому **фильтру**, событие не публикуется вовсе — запись уходит в **enrichment DLQ** (фильтр никогда
не трактуется как «ложь по умолчанию»). Поэтому наличие `PARTIAL`/`missingFields` не означает, что
объект «не совпал» — он совпал, просто часть выводимых данных недоступна.

---

## Как совпадения вычисляются (для потребителя)

Совпадения в конверте — **предвычислены** этим сервисом; Delivery-движки их не пересчитывают.

| Вход | Условие | Выход |
|---|---|---|
| `OBJECT`, объект совпал с фильтром подписки (на обогащённых данных) | ≥ 1 совпадение | `OBJECT` с `matchedSubscriptionIds` |
| `BEFORE_AFTER`, подписка совпала на `before` и/или `after` | ≥ 1 совпадение | `BEFORE_AFTER` с `subscriptionMatches` (флаги `beforeMatched`/`afterMatched`) |
| нет кандидатов / нет совпадений | — | ничего не публикуется (drop) |

Одна `BEFORE_AFTER`-запись описывает и оставшиеся в выборке подписки (`afterMatched=true`), и вышедшие
из неё (`beforeMatched=true, afterMatched=false`) — движок сам решает, как это доставить потребителю.

## Как это читают Delivery-движки

- **Ключ = `objectId`.** Все конверты одного объекта в одной партиции и упорядочены; движок применяет
  их по порядку.
- **Совпадения — готовые.** Движок оставляет из `matchedSubscriptionIds` / `subscriptionMatches` те
  подписки, что обслуживает сам (свой тип движка и `ACTIVE`), и не пересчитывает фильтр.
- **Проекция — на стороне движка.** Этот сервис отдаёт обогащённый объект **целиком**; отбор нужных
  полей (проекцию по `fields` подписки) делают Delivery-движки.
- **`metadata` прокидывается дальше** движками к потребителю без изменений.
- **Дедуп по `id`.** Доставка at-least-once; ключ дедупа — `id` версии.

См. также: [архитектура](architecture.md) · [конфигурация](configuration.md) · [эксплуатация](operations.md)
