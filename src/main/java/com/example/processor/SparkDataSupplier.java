package com.example.processor;

import com.example.util.SparkConstant;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.function.Supplier;

public class SparkDataSupplier implements Supplier<Dataset<Row>> {

  private final SparkSession spark;

  public SparkDataSupplier(SparkSession spark) {
    this.spark = spark;
  }

  @Override
  public Dataset<Row> get() {
    Dataset<Row> dataset =
        spark
            .readStream()
            .format(SparkConstant.SOCKET)
            .option(SparkConstant.HOST, SparkConstant.HOST_NAME)
            .option(SparkConstant.PORT, SparkConstant.PORT_NUMBER)
            .load();
    dataset.printSchema();
    return dataset;
  }
}
