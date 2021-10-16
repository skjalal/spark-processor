package com.example.config;

import com.example.util.SparkConstant;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class SparkConfiguration {

  private final SparkProperties sparkProperties;
  private final String tempDir;
  private SparkSession sparkSession;

  public SparkConfiguration(SparkProperties sparkProperties) {
    this.sparkProperties = sparkProperties;
    this.tempDir = System.getProperty("java.io.tmpdir");
  }

  public SparkSession getSparkSession() {
    log.info("Initializing Spark Session");
    String sparkAppName = sparkProperties.getPropertyValue(SparkConstant.SPARK_APP_NAME);
    if (sparkSession == null) {
      sparkSession = SparkSession.builder().appName(sparkAppName).config(sparkConf()).getOrCreate();
    }
    return sparkSession;
  }

  private SparkConf sparkConf() {
    SparkConf sparkConf = new SparkConf();
    sparkConf.set(SparkConstant.SPARK_MASTER, sparkProperties.getPropertyValue(SparkConstant.SPARK_MASTER));
    sparkConf.set(SparkConstant.SPARK_LOCAL_DIR, tempDir);
    sparkConf.set(SparkConstant.SPARK_DRIVER_CORES, sparkProperties.getPropertyValue(SparkConstant.SPARK_DRIVER_CORES));
    sparkConf.set(SparkConstant.SPARK_DRIVER_MAX_RESULT_SIZE, sparkProperties.getPropertyValue(SparkConstant.SPARK_DRIVER_MAX_RESULT_SIZE));
    sparkConf.set(SparkConstant.SPARK_DRIVER_MEMORY, sparkProperties.getPropertyValue(SparkConstant.SPARK_DRIVER_MEMORY));
    sparkConf.set(SparkConstant.SPARK_EXECUTOR_MEMORY, sparkProperties.getPropertyValue(SparkConstant.SPARK_EXECUTOR_MEMORY));
    sparkConf.set(SparkConstant.SPARK_SQL_SHUFFLE_PARTITIONS, sparkProperties.getPropertyValue(SparkConstant.SPARK_SQL_SHUFFLE_PARTITIONS));
    sparkConf.set(SparkConstant.SPARK_SQL_WAREHOUSE_DIR, tempDir);
    return sparkConf;
  }

  public void closeSparkSession() {
    if (sparkSession != null) {
      sparkSession.close();
      sparkSession = null;
    }
  }
}
