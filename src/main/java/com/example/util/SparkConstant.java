package com.example.util;

public final class SparkConstant {

  public static final String SPARK_MASTER = "spark.master";
  public static final String SPARK_APP_NAME = "spark.app.name";
  public static final String SPARK_DRIVER_MEMORY = "spark.driver.memory";
  public static final String SPARK_EXECUTOR_MEMORY = "spark.executor.memory";
  public static final String SPARK_DRIVER_MAX_RESULT_SIZE = "spark.driver.maxResultSize";
  public static final String SPARK_DRIVER_CORES = "spark.driver.cores";
  public static final String SPARK_LOCAL_DIR = "spark.local.dir";
  public static final String SPARK_SQL_SHUFFLE_PARTITIONS = "spark.sql.shuffle.partitions";
  public static final String SPARK_SQL_WAREHOUSE_DIR = "spark.sql.warehouse.dir";
  public static final String SPARK_STREAMING_QUERY_TIME_OUT_MS = "spark.streaming.query.timeout";
  public static final String SPARK_CONSOLE = "console";
  public static final String SOCKET = "socket";
  public static final String HOST = "host";
  public static final String HOST_NAME = "127.0.0.1";
  public static final String PORT = "port";
  public static final String PORT_NUMBER = "9090";
  public static final String CHECKPOINT_LOCATION = "checkpointLocation";
  public static final String MODE = "append";
  public static final String SPARK_CHECKPOINT_LOCATION = "spark.checkPointLocation";

  private SparkConstant() {
    throw new UnsupportedOperationException();
  }
}
