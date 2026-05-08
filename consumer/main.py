import io
import json
import logging
import os
import signal
import struct
import sys
import time
import traceback
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

import requests
from fastavro import parse_schema, schemaless_reader

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.query import BatchStatement, BatchType, SimpleStatement
from confluent_kafka import Consumer, Producer, KafkaException
from prometheus_client import Counter, Gauge, Histogram, CONTENT_TYPE_LATEST, generate_latest


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    stream=sys.stdout,
)

logger = logging.getLogger("warehouse-consumer")


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "warehouse-events")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "warehouse-state-consumer")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "warehouse-events-dlq")

CASSANDRA_HOSTS = os.getenv("CASSANDRA_HOSTS", "cassandra-1,cassandra-2,cassandra-3").split(",")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "warehouse")

METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))

SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
schema_cache = {}


CONSUMER_LAG = Gauge(
    "consumer_lag",
    "Kafka consumer lag by topic and partition",
    ["topic", "partition"],
)

EVENTS_PROCESSED_TOTAL = Counter(
    "events_processed_total",
    "Total number of successfully processed events",
    ["event_type"],
)

EVENT_PROCESSING_DURATION_SECONDS = Histogram(
    "event_processing_duration_seconds",
    "Event processing duration in seconds",
    ["event_type"],
)

CASSANDRA_WRITE_ERRORS_TOTAL = Counter(
    "cassandra_write_errors_total",
    "Total number of Cassandra write errors",
)

health_status = {
    "kafka": False,
    "cassandra": False,
}


running = True


def handle_shutdown(signum, frame):
    global running
    logger.info("Shutdown signal received. Stopping consumer...")
    running = False


signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)


class MetricsAndHealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/metrics":
            payload = generate_latest()

            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE_LATEST)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        if self.path == "/health":
            is_healthy = health_status["kafka"] and health_status["cassandra"]

            if is_healthy:
                status_code = 200
                body = b'{"status":"ok","kafka":true,"cassandra":true}'
            else:
                status_code = 503
                body = (
                    f'{{"status":"unhealthy",'
                    f'"kafka":{str(health_status["kafka"]).lower()},'
                    f'"cassandra":{str(health_status["cassandra"]).lower()}}}'
                ).encode("utf-8")

            self.send_response(status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        self.send_response(404)
        self.end_headers()

    def log_message(self, format, *args):
        return


def start_http_metrics_server():
    server = HTTPServer(("0.0.0.0", METRICS_PORT), MetricsAndHealthHandler)

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    logger.info("Metrics and health server started on port=%s", METRICS_PORT)


def wait_for_cassandra():
    last_error = None

    for attempt in range(1, 31):
        try:
            cluster = Cluster(
                CASSANDRA_HOSTS,
                protocol_version=5,
                load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1"),
            )
            session = cluster.connect()
            session.set_keyspace(CASSANDRA_KEYSPACE)

            health_status["cassandra"] = True
            logger.info("Connected to Cassandra keyspace=%s", CASSANDRA_KEYSPACE)
            return cluster, session

        except Exception as exc:
            last_error = exc
            logger.warning(
                "Cassandra is not ready yet. Attempt %s/30. Error: %s",
                attempt,
                exc,
            )
            time.sleep(5)

    raise RuntimeError(f"Could not connect to Cassandra: {last_error}")


def read_query(cql):
    return SimpleStatement(
        cql,
        consistency_level=ConsistencyLevel.QUORUM,
    )


def write_batch():
    return BatchStatement(
        batch_type=BatchType.LOGGED,
        consistency_level=ConsistencyLevel.QUORUM,
    )


def create_kafka_consumer():
    config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_GROUP_ID,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "max.poll.interval.ms": 300000,
    }

    consumer = Consumer(config)
    consumer.subscribe([KAFKA_TOPIC])

    health_status["kafka"] = True

    logger.info(
        "Kafka consumer started. topic=%s group_id=%s bootstrap_servers=%s",
        KAFKA_TOPIC,
        KAFKA_GROUP_ID,
        KAFKA_BOOTSTRAP_SERVERS,
    )

    return consumer


def create_dlq_producer():
    config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "warehouse-dlq-producer",
    }

    producer = Producer(config)

    logger.info(
        "DLQ producer started. topic=%s bootstrap_servers=%s",
        DLQ_TOPIC,
        KAFKA_BOOTSTRAP_SERVERS,
    )

    return producer


def update_consumer_lag(consumer):
    try:
        assignments = consumer.assignment()

        if not assignments:
            return

        committed_offsets = consumer.committed(assignments, timeout=5.0)

        committed_by_partition = {
            (item.topic, item.partition): item.offset
            for item in committed_offsets
        }

        for topic_partition in assignments:
            low_offset, high_offset = consumer.get_watermark_offsets(
                topic_partition,
                timeout=5.0,
                cached=False,
            )

            committed_offset = committed_by_partition.get(
                (topic_partition.topic, topic_partition.partition),
                0,
            )

            if committed_offset is None or committed_offset < 0:
                committed_offset = 0

            lag = max(high_offset - committed_offset, 0)

            CONSUMER_LAG.labels(
                topic=topic_partition.topic,
                partition=str(topic_partition.partition),
            ).set(lag)

    except Exception as exc:
        logger.warning("Failed to update consumer lag metric: %s", exc)


def classify_error(exc):
    text = str(exc)

    if isinstance(exc, ValueError):
        return "VALIDATION_ERROR"

    if "Failed to decode JSON event" in text:
        return "DESERIALIZATION_ERROR"

    return "PROCESSING_ERROR"


def extract_original_event(message):
    raw_value = message.value()

    if raw_value is None:
        return None

    raw_text = raw_value.decode("utf-8", errors="replace")

    try:
        return json.loads(raw_text)
    except Exception:
        return {
            "raw_payload": raw_text
        }


def send_to_dlq(dlq_producer, message, exc):
    failed_at = datetime.now(timezone.utc).isoformat()

    dlq_payload = {
        "original_event": extract_original_event(message),
        "error_reason": str(exc),
        "error_code": classify_error(exc),
        "failed_at": failed_at,
        "kafka_metadata": {
            "topic": message.topic(),
            "partition": int(message.partition()),
            "offset": int(message.offset()),
        },
        "stacktrace": traceback.format_exc(),
    }

    delivery_error = [None]

    def delivery_callback(err, msg):
        if err is not None:
            delivery_error[0] = err

    dlq_producer.produce(
        topic=DLQ_TOPIC,
        key=str(message.key()).encode("utf-8") if message.key() is not None else None,
        value=json.dumps(dlq_payload, ensure_ascii=False).encode("utf-8"),
        callback=delivery_callback,
    )

    dlq_producer.flush(timeout=10)

    if delivery_error[0] is not None:
        raise RuntimeError(f"Failed to deliver message to DLQ: {delivery_error[0]}")

    logger.info(
        "Event sent to DLQ. dlq_topic=%s original_partition=%s original_offset=%s error_code=%s",
        DLQ_TOPIC,
        message.partition(),
        message.offset(),
        dlq_payload["error_code"],
    )


def get_avro_schema_by_id(schema_id):
    if schema_id in schema_cache:
        return schema_cache[schema_id]

    response = requests.get(
        f"{SCHEMA_REGISTRY_URL}/schemas/ids/{schema_id}",
        timeout=5,
    )
    response.raise_for_status()

    schema = response.json()["schema"]
    schema_dict = json.loads(schema)
    parsed_schema = parse_schema(schema_dict)

    field_names = {field["name"] for field in schema_dict.get("fields", [])}

    if "supplier_id" in field_names:
        schema_version = "v2"
    else:
        schema_version = "v1"

    schema_cache[schema_id] = {
        "parsed_schema": parsed_schema,
        "schema_version": schema_version,
    }

    return schema_cache[schema_id]


def decode_confluent_avro(raw_value):
    if len(raw_value) < 5:
        raise ValueError("Invalid Avro payload: too short")

    magic_byte = raw_value[0]

    if magic_byte != 0:
        raise ValueError("Invalid Confluent Avro payload: wrong magic byte")

    schema_id = struct.unpack(">I", raw_value[1:5])[0]

    schema_info = get_avro_schema_by_id(schema_id)

    bio = io.BytesIO(raw_value[5:])
    event = schemaless_reader(bio, schema_info["parsed_schema"])

    event["_schema_id"] = schema_id
    event["_schema_version"] = schema_info["schema_version"]

    return event


def decode_event(message):
    raw_value = message.value()

    if raw_value is None:
        raise ValueError("Kafka message value is empty")

    if len(raw_value) >= 5 and raw_value[0] == 0:
        return decode_confluent_avro(raw_value)

    try:
        return json.loads(raw_value.decode("utf-8"))
    except Exception as exc:
        raise ValueError(f"Failed to decode JSON or Avro event: {exc}") from exc


def validate_event(event):
    required_fields = ["event_id", "event_type"]

    for field in required_fields:
        if field not in event:
            raise ValueError(f"Missing required field: {field}")

    if not event["event_id"]:
        raise ValueError("event_id must not be empty")

    if not event["event_type"]:
        raise ValueError("event_type must not be empty")


def parse_event_timestamp(event):
    raw_timestamp = event.get("event_timestamp") or event.get("timestamp")

    if raw_timestamp is None:
        return datetime.now(timezone.utc)

    if isinstance(raw_timestamp, (int, float)):
        return datetime.fromtimestamp(raw_timestamp / 1000, tz=timezone.utc)

    raw_timestamp = str(raw_timestamp)

    if raw_timestamp.endswith("Z"):
        raw_timestamp = raw_timestamp.replace("Z", "+00:00")

    parsed = datetime.fromisoformat(raw_timestamp)

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def get_last_product_event_timestamp(session, product_id):
    row = session.execute(
        read_query(
            """
            SELECT last_event_timestamp
            FROM warehouse.latest_event_by_product
            WHERE product_id = %s
            """
        ),
        (product_id,),
    ).one()

    if row is None:
        return None

    return row.last_event_timestamp


def write_latest_product_event(batch, product_id, event, event_timestamp, updated_at):
    batch.add(
        """
        INSERT INTO warehouse.latest_event_by_product (
            product_id,
            last_event_timestamp,
            last_event_id,
            last_event_type,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s)
        """,
        (
            product_id,
            event_timestamp,
            str(event["event_id"]),
            str(event["event_type"]),
            updated_at,
        ),
    )


def get_product_id(event):
    product_id = event.get("product_id") or event.get("sku")

    if not product_id:
        raise ValueError("Missing product_id or sku")

    return str(product_id)


def get_zone_id(event):
    zone_id = event.get("zone_id")

    if not zone_id:
        raise ValueError("Missing zone_id")

    return str(zone_id)


def get_quantity(event):
    quantity = event.get("quantity")

    if quantity is None:
        raise ValueError("Missing quantity")

    quantity = int(quantity)

    if quantity <= 0:
        raise ValueError("quantity must be positive")

    return quantity


def get_current_inventory(session, product_id, zone_id):
    row = session.execute(
        read_query(
            """
            SELECT available_quantity, reserved_quantity, supplier_id
            FROM warehouse.inventory_by_product_zone
            WHERE product_id = %s AND zone_id = %s
            """
        ),
        (product_id, zone_id),
    ).one()

    if row is None:
        return 0, 0, None

    return (
        int(row.available_quantity or 0), 
        int(row.reserved_quantity or 0),
        row.supplier_id,
    )


def get_total_inventory(session, product_id):
    row = session.execute(
        read_query(
            """
            SELECT total_available_quantity, total_reserved_quantity
            FROM warehouse.inventory_totals_by_product
            WHERE product_id = %s
            """
        ),
        (product_id,),
    ).one()

    if row is None:
        return 0, 0

    return int(row.total_available_quantity or 0), int(row.total_reserved_quantity or 0)


def write_inventory_state(
    batch, 
    product_id, 
    zone_id, 
    available_quantity, 
    reserved_quantity, 
    updated_at,
    supplier_id=None,
):
    batch.add(
        """
        INSERT INTO warehouse.inventory_by_product_zone (
            product_id,
            zone_id,
            available_quantity,
            reserved_quantity,
            supplier_id,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (product_id, zone_id, available_quantity, reserved_quantity, supplier_id, updated_at),
    )

    batch.add(
        """
        INSERT INTO warehouse.inventory_by_product (
            product_id,
            zone_id,
            available_quantity,
            reserved_quantity,
            supplier_id,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (product_id, zone_id, available_quantity, reserved_quantity, supplier_id, updated_at),
    )

    batch.add(
        """
        INSERT INTO warehouse.inventory_by_zone (
            zone_id,
            product_id,
            available_quantity,
            reserved_quantity,
            supplier_id,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (zone_id, product_id, available_quantity, reserved_quantity, supplier_id, updated_at),
    )


def write_total_state(batch, product_id, total_available_quantity, total_reserved_quantity, updated_at):
    batch.add(
        """
        INSERT INTO warehouse.inventory_totals_by_product (
            product_id,
            total_available_quantity,
            total_reserved_quantity,
            updated_at
        )
        VALUES (%s, %s, %s, %s)
        """,
        (product_id, total_available_quantity, total_reserved_quantity, updated_at),
    )


def write_event_history(batch, event, product_id, zone_id, quantity, event_time):
    supplier_id = event.get("supplier_id")

    batch.add(
        """
        INSERT INTO warehouse.events_by_product (
            product_id,
            event_time,
            event_id,
            event_type,
            zone_id,
            quantity,
            supplier_id,
            raw_event
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            product_id,
            event_time,
            str(event["event_id"]),
            str(event["event_type"]),
            zone_id,
            quantity,
            supplier_id,
            json.dumps(event, ensure_ascii=False),
        ),
    )


def mark_event_processed(batch, event, message, product_id, zone_id, quantity, processed_at):
    batch.add(
        """
        INSERT INTO warehouse.processed_events (
            event_id,
            event_type,
            product_id,
            zone_id,
            quantity,
            kafka_partition,
            kafka_offset,
            processed_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            str(event["event_id"]),
            str(event["event_type"]),
            product_id,
            zone_id,
            quantity,
            int(message.partition()),
            int(message.offset()),
            processed_at,
        ),
    )


def is_event_already_processed(session, event_id):
    row = session.execute(
        read_query(
            """
            SELECT event_id
            FROM warehouse.processed_events
            WHERE event_id = %s
            """
        ),
        (str(event_id),),
    ).one()

    return row is not None


def apply_product_received(session, batch, event, now):
    product_id = get_product_id(event)
    zone_id = get_zone_id(event)
    quantity = get_quantity(event)

    available, reserved, existing_supplier_id = get_current_inventory(session, product_id, zone_id)
    total_available, total_reserved = get_total_inventory(session, product_id)

    supplier_id = event.get("supplier_id")

    new_available = available + quantity
    new_reserved = reserved

    new_total_available = total_available + quantity
    new_total_reserved = total_reserved

    write_inventory_state(
        batch, 
        product_id, 
        zone_id, 
        new_available, 
        new_reserved, 
        now,
        supplier_id=supplier_id,
    )
    
    write_total_state(batch, product_id, new_total_available, new_total_reserved, now)
    write_event_history(batch, event, product_id, zone_id, quantity, now)
    
    logger.info(
        "ProductReceived handled. schema_version=%s schema_id=%s supplier_id=%s",
        event.get("_schema_version", "json"),
        event.get("_schema_id"),
        supplier_id,
    )

    return product_id, zone_id, quantity


def apply_product_shipped(session, batch, event, now):
    product_id = get_product_id(event)
    zone_id = get_zone_id(event)
    quantity = get_quantity(event)

    available, reserved, supplier_id = get_current_inventory(session, product_id, zone_id)
    total_available, total_reserved = get_total_inventory(session, product_id)

    if available < quantity:
        raise ValueError(
            f"Not enough available inventory for shipping. product_id={product_id} zone_id={zone_id}"
        )

    new_available = available - quantity
    new_reserved = reserved

    new_total_available = total_available - quantity
    new_total_reserved = total_reserved

    write_inventory_state(batch, product_id, zone_id, new_available, new_reserved, now, supplier_id=supplier_id)
    write_total_state(batch, product_id, new_total_available, new_total_reserved, now)
    write_event_history(batch, event, product_id, zone_id, quantity, now)

    return product_id, zone_id, quantity


def apply_product_reserved(session, batch, event, now):
    product_id = get_product_id(event)
    zone_id = get_zone_id(event)
    quantity = get_quantity(event)

    available, reserved, supplier_id = get_current_inventory(session, product_id, zone_id)
    total_available, total_reserved = get_total_inventory(session, product_id)

    if available < quantity:
        raise ValueError(
            f"Not enough available inventory for reservation. product_id={product_id} zone_id={zone_id}"
        )

    new_available = available - quantity
    new_reserved = reserved + quantity

    new_total_available = total_available - quantity
    new_total_reserved = total_reserved + quantity

    write_inventory_state(batch, product_id, zone_id, new_available, new_reserved, now, supplier_id=supplier_id)
    write_total_state(batch, product_id, new_total_available, new_total_reserved, now)
    write_event_history(batch, event, product_id, zone_id, quantity, now)

    return product_id, zone_id, quantity


def apply_product_released(session, batch, event, now):
    product_id = get_product_id(event)
    zone_id = get_zone_id(event)
    quantity = get_quantity(event)

    available, reserved, supplier_id = get_current_inventory(session, product_id, zone_id)
    total_available, total_reserved = get_total_inventory(session, product_id)

    if reserved < quantity:
        raise ValueError(
            f"Not enough reserved inventory for release. product_id={product_id} zone_id={zone_id}"
        )

    new_available = available + quantity
    new_reserved = reserved - quantity

    new_total_available = total_available + quantity
    new_total_reserved = total_reserved - quantity

    write_inventory_state(batch, product_id, zone_id, new_available, new_reserved, now, supplier_id=supplier_id)
    write_total_state(batch, product_id, new_total_available, new_total_reserved, now)
    write_event_history(batch, event, product_id, zone_id, quantity, now)

    return product_id, zone_id, quantity


def apply_inventory_counted(session, batch, event, now):
    product_id = get_product_id(event)
    zone_id = get_zone_id(event)

    counted_quantity = event.get("counted_quantity")

    if counted_quantity is None:
        counted_quantity = event.get("quantity")

    counted_quantity = int(counted_quantity)

    if counted_quantity < 0:
        raise ValueError("counted_quantity must not be negative")

    available, reserved, supplier_id = get_current_inventory(session, product_id, zone_id)
    total_available, total_reserved = get_total_inventory(session, product_id)

    new_available = counted_quantity
    new_reserved = reserved

    difference = new_available - available

    new_total_available = total_available + difference
    new_total_reserved = total_reserved

    write_inventory_state(batch, product_id, zone_id, new_available, new_reserved, now, supplier_id=supplier_id)
    write_total_state(batch, product_id, new_total_available, new_total_reserved, now)
    write_event_history(batch, event, product_id, zone_id, counted_quantity, now)

    return product_id, zone_id, counted_quantity


def apply_product_moved(session, batch, event, now):
    product_id = get_product_id(event)

    from_zone_id = event.get("from_zone_id")
    to_zone_id = event.get("to_zone_id")
    quantity = get_quantity(event)

    if not from_zone_id:
        raise ValueError("Missing from_zone_id")

    if not to_zone_id:
        raise ValueError("Missing to_zone_id")

    from_zone_id = str(from_zone_id)
    to_zone_id = str(to_zone_id)

    from_available, from_reserved, from_supplier_id = get_current_inventory(
        session,
        product_id,
        from_zone_id,
    )

    to_available, to_reserved, to_supplier_id = get_current_inventory(
        session,
        product_id,
        to_zone_id,
    )

    if from_available < quantity:
        raise ValueError(
            f"Not enough available inventory for move. product_id={product_id} from_zone_id={from_zone_id}"
        )

    target_supplier_id = to_supplier_id or from_supplier_id

    write_inventory_state(
        batch,
        product_id,
        from_zone_id,
        from_available - quantity,
        from_reserved,
        now,
        supplier_id=from_supplier_id,
    )

    write_inventory_state(
        batch,
        product_id,
        to_zone_id,
        to_available + quantity,
        to_reserved,
        now,
        supplier_id=target_supplier_id,
    )

    write_event_history(batch, event, product_id, from_zone_id, quantity, now)

    return product_id, f"{from_zone_id}->{to_zone_id}", quantity


def apply_event_to_state(session, event):
    now = datetime.now(timezone.utc)
    event_timestamp = parse_event_timestamp(event)
    event_type = str(event["event_type"])

    if is_event_already_processed(session, event["event_id"]):
        logger.info(
            "Event already processed. Skipping state update. event_id=%s event_type=%s",
            event["event_id"],
            event_type,
        )
        return None, None, None, True, None

    product_id = get_product_id(event)

    last_event_timestamp = get_last_product_event_timestamp(session, product_id)

    if last_event_timestamp is not None:
        if last_event_timestamp.tzinfo is None:
            last_event_timestamp = last_event_timestamp.replace(tzinfo=timezone.utc)

        if event_timestamp <= last_event_timestamp:
            logger.info(
                "Out-of-order event ignored. event_id=%s event_type=%s product_id=%s event_timestamp=%s last_event_timestamp=%s",
                event["event_id"],
                event_type,
                product_id,
                event_timestamp.isoformat(),
                last_event_timestamp.isoformat(),
            )
            return product_id, event.get("zone_id"), event.get("quantity"), True, None

    batch = write_batch()

    if event_type == "PRODUCT_RECEIVED":
        product_id, zone_id, quantity = apply_product_received(session, batch, event, event_timestamp)

    elif event_type == "PRODUCT_SHIPPED":
        product_id, zone_id, quantity = apply_product_shipped(session, batch, event, event_timestamp)

    elif event_type == "PRODUCT_RESERVED":
        product_id, zone_id, quantity = apply_product_reserved(session, batch, event, event_timestamp)

    elif event_type == "PRODUCT_RELEASED":
        product_id, zone_id, quantity = apply_product_released(session, batch, event, event_timestamp)

    elif event_type == "INVENTORY_COUNTED":
        product_id, zone_id, quantity = apply_inventory_counted(session, batch, event, event_timestamp)

    elif event_type == "PRODUCT_MOVED":
        product_id, zone_id, quantity = apply_product_moved(session, batch, event, event_timestamp)

    else:
        raise ValueError(f"Unsupported event_type: {event_type}")

    write_latest_product_event(
        batch=batch,
        product_id=product_id,
        event=event,
        event_timestamp=event_timestamp,
        updated_at=now,
    )

    return product_id, zone_id, quantity, False, batch


def process_event(session, event, message):
    product_id, zone_id, quantity, already_processed, batch = apply_event_to_state(session, event)

    processed_at = datetime.now(timezone.utc)

    if not already_processed:
        mark_event_processed(
            batch=batch,
            event=event,
            message=message,
            product_id=product_id,
            zone_id=zone_id,
            quantity=quantity,
            processed_at=processed_at,
        )

        try:
            session.execute(batch)
            health_status["cassandra"] = True
        except Exception:
            CASSANDRA_WRITE_ERRORS_TOTAL.inc()
            health_status["cassandra"] = False
            raise

        logger.info(
            "Cassandra logged batch applied. event_id=%s event_type=%s",
            event["event_id"],
            event["event_type"],
        )

    logger.info(
        "Event processed successfully. event_id=%s event_type=%s partition=%s offset=%s",
        event["event_id"],
        event["event_type"],
        message.partition(),
        message.offset(),
    )


def main():
    cluster = None
    consumer = None
    dlq_producer = None

    start_http_metrics_server()

    try:
        cluster, session = wait_for_cassandra()
        consumer = create_kafka_consumer()
        dlq_producer = create_dlq_producer()

        while running:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                raise KafkaException(message.error())

            try:
                processing_started_at = time.perf_counter()
                event_type = "UNKNOWN"

                event = decode_event(message)
                validate_event(event)

                event_type = str(event["event_type"])

                process_event(session, event, message)

                processing_duration = time.perf_counter() - processing_started_at

                EVENT_PROCESSING_DURATION_SECONDS.labels(
                    event_type=event_type,
                    ).observe(processing_duration)

                EVENTS_PROCESSED_TOTAL.labels(
                    event_type=event_type,
                ).inc()

                consumer.commit(message=message, asynchronous=False)

                update_consumer_lag(consumer)

                logger.info(
                    "Kafka offset committed. partition=%s offset=%s",
                    message.partition(),
                    message.offset(),
                )

            except Exception as exc:
                logger.exception(
                    "Failed to process event. Sending to DLQ. partition=%s offset=%s error=%s",
                    message.partition(),
                    message.offset(),
                    exc,
                )

                try:
                    send_to_dlq(dlq_producer, message, exc)

                    consumer.commit(message=message, asynchronous=False)

                    update_consumer_lag(consumer)

                    logger.info(
                        "Kafka offset committed after DLQ. partition=%s offset=%s",
                        message.partition(),
                        message.offset(),
                    )
                
                except Exception as dlq_exc:
                    logger.exception(
                        "Failed to send event to DLQ. Offset will not be committed. partition=%s offset=%s error=%s",
                        message.partition(),
                        message.offset(),
                        dlq_exc,
                    )

                    time.sleep(3)

    finally:
        health_status["kafka"] = False
        health_status["cassandra"] = False
        
        if consumer is not None:
            logger.info("Closing Kafka consumer...")
            consumer.close()

        if cluster is not None:
            logger.info("Closing Cassandra connection...")
            cluster.shutdown()

        logger.info("Consumer stopped.")


if __name__ == "__main__":
    main()