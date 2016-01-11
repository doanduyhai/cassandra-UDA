package com.datastax.aggregation;

import static com.datastax.aggregation.SensorDataConfig.*;

import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

public class SimpleAvgFeed {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleAvgFeed.class);
    private static final String FEED_QUERY = "INSERT INTO test_uda.sensor(sensor_id,time,value) VALUES(:id,:time,:value);";

    public static void main(String...args) {
        final SimpleAvgFeed scenario = new SimpleAvgFeed();
        scenario.injectSensorDataSinglePartition(SENSOR_PARTITION_SIZE);
    }

    private final Cluster cluster;
    private final Session session;
    private final ScriptExecutor scriptExecutor;
    private final PreparedStatement insertSensorData;

    private SimpleAvgFeed() {
        this.cluster = Cluster.builder()
                .addContactPoint(CASSANDRA_HOST)
                .build();

        this.session = cluster.connect();
        this.scriptExecutor = new ScriptExecutor(session);
        prepareSchema();
        this.insertSensorData = session.prepare(FEED_QUERY);
    }

    private void prepareSchema() {
        scriptExecutor.executeScript(SCHEMA_SCRIPT);
    }

    private void injectSensorDataSinglePartition(long partitionSize) {
        final int sensorId = 10;
        int count=0;
        try {
            for (long time = 1; time <= partitionSize; time++) {
                try {
                    count++;
                    final double randomValue = RandomUtils.nextDouble(0d, 100d);
                    session.executeAsync(insertSensorData.bind(sensorId, time, randomValue));
                    if (count == THREAD_SLEEP_FOR_EVERY_N_INSERT) {
                        Thread.sleep(SLEEP_TIME_FOR_EVERY_N_INSERT_IN_MILLIS);
                        count = 0;
                    }
                    if (time % LOG_DISPLAY_FOR_EVERY_N_INSERT == 0) {
                        Thread.sleep(SLEEP_TIME_FOR_EVERY_LOG_DISPLAY_IN_MILLIS);
                        LOGGER.info(String.format("Round %s finished", Math.floor(time / LOG_DISPLAY_FOR_EVERY_N_INSERT)));
                    }
                } catch (InterruptedException e) {
                    LOGGER.error(e.getMessage());
                }
            }
        } finally {
            session.close();
            cluster.close();
        }
    }
}
