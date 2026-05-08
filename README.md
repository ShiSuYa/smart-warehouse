# Smart Warehouse

Event-driven система управления складом на Kafka + Cassandra.

Система принимает события складских операций из Kafka, обрабатывает их consumer-сервисом и сохраняет состояние склада в Cassandra.

## Стек

- Kafka
- Schema Registry
- Cassandra 3-node cluster
- Python Consumer
- Prometheus
- Grafana
- Docker Compose

## Запуск

```bash
docker compose up --build
````

Для чистого запуска:

```bash
docker compose down -v
docker compose up --build
```

После запуска доступны:

```text
Consumer health:  http://localhost:8000/health
Consumer metrics: http://localhost:8000/metrics
Prometheus:       http://localhost:9090
Grafana:          http://localhost:3000
Schema Registry:  http://localhost:8081
```

Grafana:

```text
admin / admin
```

---

## Архитектура

![Alt text](image.png)

Consumer читает события из topic:

```text
warehouse-events
```

Consumer group:

```text
warehouse-state-consumer
```

DLQ topic:

```text
warehouse-events-dlq
```

---

## Cassandra model

Keyspace:

```sql
CREATE KEYSPACE warehouse
WITH replication = {
  'class': 'NetworkTopologyStrategy',
  'datacenter1': 3
};
```

Основные таблицы:

```text
inventory_by_product_zone
inventory_by_product
inventory_by_zone
inventory_totals_by_product
events_by_product
processed_events
latest_event_by_product
```

Модель спроектирована query-first. JOIN не используются. Денормализация сделана намеренно под быстрые чтения.

### Основные запросы

Остаток товара в конкретной зоне:

```sql
SELECT * FROM warehouse.inventory_by_product_zone
WHERE product_id = ? AND zone_id = ?;
```

Все зоны товара:

```sql
SELECT * FROM warehouse.inventory_by_product
WHERE product_id = ?;
```

Все товары в зоне:

```sql
SELECT * FROM warehouse.inventory_by_zone
WHERE zone_id = ?;
```

Агрегированный остаток товара:

```sql
SELECT * FROM warehouse.inventory_totals_by_product
WHERE product_id = ?;
```

---

## Обработка событий

Поддерживаются события:

```text
PRODUCT_RECEIVED
PRODUCT_SHIPPED
PRODUCT_MOVED
PRODUCT_RESERVED
PRODUCT_RELEASED
INVENTORY_COUNTED
```

Примеры логики:

```text
PRODUCT_RECEIVED:
available += quantity

PRODUCT_RESERVED:
available -= quantity
reserved += quantity

PRODUCT_RELEASED:
available += quantity
reserved -= quantity

PRODUCT_SHIPPED:
available -= quantity

PRODUCT_MOVED:
from_zone.available -= quantity
to_zone.available += quantity
```

---

## At-least-once semantics

Consumer отключает auto commit offset.

Offset коммитится только после успешной обработки события и записи в Cassandra.

```text
read event -> process -> write Cassandra -> commit Kafka offset
```

Это даёт at-least-once delivery.

---

## Idempotency

Идемпотентность реализована через таблицу:

```text
processed_events
```

Перед обработкой consumer проверяет `event_id`.

Если событие уже обработано, состояние не меняется повторно.

Пример:

```text
Первое событие PRODUCT_RECEIVED +50:
available = 50

Повтор того же event_id:
available = 50
```

---

## Consistency between denormalized tables

Для обновления связанных таблиц используется Cassandra logged batch:

```python
BatchStatement(
    batch_type=BatchType.LOGGED,
    consistency_level=ConsistencyLevel.QUORUM
)
```

Одно событие обновляет сразу:

```text
inventory_by_product_zone
inventory_by_product
inventory_by_zone
inventory_totals_by_product
events_by_product
processed_events
latest_event_by_product
```

Это защищает от частичных обновлений.

---

## Out-of-order events

Для обработки событий вне порядка используется поле:

```text
event_timestamp
```

Consumer хранит последний timestamp по товару в таблице:

```text
latest_event_by_product
```

Если приходит старое событие, оно игнорируется.

Пример:

```text
12:00 PRODUCT_RECEIVED +100 -> available = 100
12:05 PRODUCT_SHIPPED -20  -> available = 80
12:02 PRODUCT_RECEIVED +50 -> ignored
```

Итог:

```text
available = 80
```

---

## Dead Letter Queue

Если событие невалидное, consumer не падает и не блокируется.

Плохое событие отправляется в:

```text
warehouse-events-dlq
```

DLQ payload содержит:

```json
{
  "original_event": {},
  "error_reason": "quantity must be positive",
  "error_code": "VALIDATION_ERROR",
  "failed_at": "2026-05-07T13:03:39Z",
  "kafka_metadata": {
    "topic": "warehouse-events",
    "partition": 0,
    "offset": 0
  },
  "stacktrace": "..."
}
```

После успешной отправки в DLQ offset коммитится.

---

## Cassandra cluster

Проект запускает 3 Cassandra-ноды:

```text
cassandra-1
cassandra-2
cassandra-3
```

Проверка:

```bash
docker exec -it cassandra-1 nodetool status
```

Ожидаемо:

```text
UN cassandra-1
UN cassandra-2
UN cassandra-3
```

Replication factor:

```text
3
```

Consistency levels:

```text
Writes: QUORUM
Reads:  QUORUM
```

При RF=3:

```text
QUORUM = 2 replicas
```

Выбран `QUORUM`, потому что consumer читает важное состояние: остатки, `processed_events`, `latest_event_by_product`. Stale read может привести к неправильному состоянию склада.

Проверка отказоустойчивости:

```bash
docker stop cassandra-2
```

После остановки одной ноды consumer продолжает обрабатывать события, потому что две ноды всё ещё доступны и удовлетворяют `QUORUM`.

---

## Monitoring

Consumer предоставляет:

```text
/health
/metrics
```

Health:

```bash
curl http://localhost:8000/health
```

Пример ответа:

```json
{"status":"ok","kafka":true,"cassandra":true}
```

Metrics:

```bash
curl http://localhost:8000/metrics
```

Метрики:

```text
consumer_lag
events_processed_total
event_processing_duration_seconds
cassandra_write_errors_total
```

Prometheus:

```text
http://localhost:9090
```

Grafana:

```text
http://localhost:3000
```

Dashboard:

```text
Smart Warehouse Consumer
```

Панели:

```text
Consumer lag by partition
Throughput — events processed per second
Cassandra write errors
Event processing duration p95
```

---

## Schema Evolution

Используется Avro + Schema Registry.

Subject:

```text
warehouse-events-value
```

Compatibility:

```text
BACKWARD
```

Проверка:

```bash
curl http://localhost:8081/config/warehouse-events-value
```

```bash
curl http://localhost:8081/subjects/warehouse-events-value/versions
```

Ожидаемо:

```json
[1,2]
```

### V1 ProductReceived

Поля:

```text
event_id
event_type
product_id
zone_id
quantity
event_timestamp
```

Для V1:

```text
supplier_id = null
```

### V2 ProductReceived

V2 добавляет поле:

```text
supplier_id
```

Поле backward-compatible, потому что optional и имеет default:

```json
{
  "name": "supplier_id",
  "type": ["null", "string"],
  "default": null
}
```

Для V2:

```text
supplier_id = SUP-001
```

Проверенные логи:

```text
ProductReceived handled. schema_version=v1 schema_id=1 supplier_id=None
ProductReceived handled. schema_version=v2 schema_id=2 supplier_id=SUP-001
```

---

## Примеры команд

Отправить обычное JSON-событие:

```bash
docker exec -it warehouse-kafka kafka-console-producer \
  --bootstrap-server kafka:9092 \
  --topic warehouse-events
```

Пример события:

```json
{"event_id":"event-001","event_type":"PRODUCT_RECEIVED","product_id":"SKU-001","zone_id":"ZONE-A","quantity":100,"event_timestamp":"2026-05-05T12:00:00Z"}
```

Проверить Cassandra:

```bash
docker exec -it cassandra-1 cqlsh
```

```sql
SELECT * FROM warehouse.inventory_by_product_zone
WHERE product_id = 'SKU-001' AND zone_id = 'ZONE-A';
```

Прочитать DLQ:

```bash
docker exec -it warehouse-kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic warehouse-events-dlq \
  --from-beginning \
  --max-messages 1
```

---

## Итог

Реализованы все пункты задания:

```text
1. Kafka consumer
2. Cassandra data model
3. Обработка событий с записью состояния
4. Идемпотентность
5. Cassandra logged batch
6. Out-of-order events
7. Dead Letter Queue
8. Cassandra 3-node cluster
9. Monitoring + Grafana
10. Schema Evolution