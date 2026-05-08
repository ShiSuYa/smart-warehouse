CREATE KEYSPACE IF NOT EXISTS warehouse
WITH replication = {
  'class': 'NetworkTopologyStrategy',
  'datacenter1': 3
};

CREATE TABLE IF NOT EXISTS warehouse.inventory_by_product_zone (
    product_id text,
    zone_id text,
    available_quantity int,
    reserved_quantity int,
    supplier_id text,
    updated_at timestamp,
    PRIMARY KEY ((product_id), zone_id)
);

CREATE TABLE IF NOT EXISTS warehouse.inventory_by_product (
    product_id text,
    zone_id text,
    available_quantity int,
    reserved_quantity int,
    supplier_id text,
    updated_at timestamp,
    PRIMARY KEY ((product_id), zone_id)
);

CREATE TABLE IF NOT EXISTS warehouse.inventory_by_zone (
    zone_id text,
    product_id text,
    available_quantity int,
    reserved_quantity int,
    supplier_id text,
    updated_at timestamp,
    PRIMARY KEY ((zone_id), product_id)
);

CREATE TABLE IF NOT EXISTS warehouse.inventory_totals_by_product (
    product_id text PRIMARY KEY,
    total_available_quantity int,
    total_reserved_quantity int,
    updated_at timestamp
);

CREATE TABLE IF NOT EXISTS warehouse.events_by_product (
    product_id text,
    event_time timestamp,
    event_id text,
    event_type text,
    zone_id text,
    quantity int,
    supplier_id text,
    raw_event text,
    PRIMARY KEY ((product_id), event_time, event_id)
) WITH CLUSTERING ORDER BY (event_time DESC);

CREATE TABLE IF NOT EXISTS warehouse.processed_events (
    event_id text PRIMARY KEY,
    event_type text,
    product_id text,
    zone_id text,
    quantity int,
    kafka_partition int,
    kafka_offset bigint,
    processed_at timestamp
);

CREATE TABLE IF NOT EXISTS warehouse.latest_event_by_product (
    product_id text PRIMARY KEY,
    last_event_timestamp timestamp,
    last_event_id text,
    last_event_type text,
    updated_at timestamp
);