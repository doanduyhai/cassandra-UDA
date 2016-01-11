package com.datastax.aggregation;

import static com.datastax.aggregation.GlobalConfig.CASSANDRA_HOST;
import static com.datastax.aggregation.SensorDataConfig.DRIVER_READ_TIMEOUT_IN_MILLIS;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.*;

public class SimpleAvgRead {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleAvgRead.class);
    private static final String FULL_TABLE_SCAN = "SELECT avg(value) FROM test_uda.sensor;";
    private static final String FULL_PARTITION_SCAN = "SELECT avg(value) FROM test_uda.sensor WHERE sensor_id=?;";
    private static final String PARTITION_SCAN_BY_CLUSTERING = "SELECT avg(value) FROM test_uda.sensor WHERE sensor_id=? AND time>=? AND time<=?;";

    public static void main(String...args) {
        final SimpleAvgRead scenario = new SimpleAvgRead();
        final PreparedStatement fullTableScanPs = scenario.session.prepare(FULL_TABLE_SCAN);
        final PreparedStatement fullPartitionScanPs = scenario.session.prepare(FULL_PARTITION_SCAN);
        final PreparedStatement scanWithClusteringPs = scenario.session.prepare(PARTITION_SCAN_BY_CLUSTERING);
        scenario.readAverage(fullPartitionScanPs.bind(10), 50_000);
    }

    private final Cluster cluster;
    private final Session session;


    private SimpleAvgRead() {
        final SocketOptions socketOptions = new SocketOptions();
        socketOptions.setReadTimeoutMillis(DRIVER_READ_TIMEOUT_IN_MILLIS);
        this.cluster = Cluster.builder()
                .addContactPoint(CASSANDRA_HOST)
                .withSocketOptions(socketOptions)
                .build();
        this.session = cluster.connect();
    }


    private void readAverage(BoundStatement boundStatement, int pageSize) {
        try {
            boundStatement.setFetchSize(pageSize);
            boundStatement.enableTracing();
            final ResultSet resultSet = session.execute(boundStatement);
            final Row one = resultSet.one();
            LOGGER.info("Average value = "+one.getDouble("system.avg(value)"));
            LOGGER.info("Query tracing: \n"+resultSet.getExecutionInfo().getQueryTrace().toString());

        } finally {
            session.close();
            cluster.close();
        }
    }
}
