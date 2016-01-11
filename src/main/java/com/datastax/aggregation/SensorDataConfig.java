package com.datastax.aggregation;

public interface SensorDataConfig extends GlobalConfig {

    // Feed
    long SENSOR_PARTITION_SIZE = 10_000_000L;

    int THREAD_SLEEP_FOR_EVERY_N_INSERT = 500; //Sleep every 500 async inserts
    int SLEEP_TIME_FOR_EVERY_N_INSERT_IN_MILLIS = 10; //Sleep 10 millis every N async inserts
    int LOG_DISPLAY_FOR_EVERY_N_INSERT = 1_000_000; //Display log for every million inserts
    int SLEEP_TIME_FOR_EVERY_LOG_DISPLAY_IN_MILLIS = 10_000; //Sleep 10 secs for every log display

    //Read
    int DRIVER_READ_TIMEOUT_IN_MILLIS = 200_000;

}
