package com.example.processor;

import com.example.config.SparkProperties;
import com.example.util.SparkConstant;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

@Slf4j
public class SparkDataConsumer implements Consumer<Dataset<String>> {

  private final SparkProperties sparkProperties;
  private final long queryTimeOut;

  public SparkDataConsumer(SparkProperties sparkProperties) {
    this.sparkProperties = sparkProperties;
    this.queryTimeOut =
        Long.parseLong(
            sparkProperties.getPropertyValue(SparkConstant.SPARK_STREAMING_QUERY_TIME_OUT_MS));
  }

  @Override
  public void accept(Dataset<String> dataset) {
    try {
      dataset
          .writeStream()
          .format(SparkConstant.SPARK_CONSOLE)
          .outputMode(SparkConstant.MODE)
          .option(
              SparkConstant.CHECKPOINT_LOCATION,
              sparkProperties.getPropertyValue(SparkConstant.SPARK_CHECKPOINT_LOCATION))
          .start()
          .awaitTermination(queryTimeOut);
    } catch (TimeoutException | StreamingQueryException e) {
      log.error("Failed on streaming data", e);
    }
  }
}
