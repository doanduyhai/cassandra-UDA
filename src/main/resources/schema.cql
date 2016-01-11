CREATE KEYSPACE IF NOT EXISTS test_uda WITH REPLICATION = {
	'class' : 'org.apache.cassandra.locator.SimpleStrategy',
	'replication_factor': '1' }
AND DURABLE_WRITES = true;

CREATE TABLE IF NOT EXISTS test_uda.sensor(
    sensor_id int,
    time bigint,
    value double,
    PRIMARY KEY(sensor_id, time)
);

TRUNCATE test_uda.sensor;
