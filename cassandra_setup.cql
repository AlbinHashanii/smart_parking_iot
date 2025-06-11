CREATE KEYSPACE parking_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE parking_keyspace.raw_data (
    sensor_id text,
    slot_id text,
    occupied boolean,
    timestamp timestamp,
    PRIMARY KEY (sensor_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

CREATE TABLE parking_keyspace.metrics (
    timestamp timestamp,
    free_slots int,
    PRIMARY KEY (timestamp)
);

CREATE TABLE parking_keyspace.metadata (
    sensor_id text,
    slot_id text PRIMARY KEY,
    x int,
    y int
);

CREATE TABLE parking_keyspace.predictions (
    date text PRIMARY KEY,
    free_slots int
);