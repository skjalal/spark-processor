package com.example.service;

import com.example.config.SparkProperties;
import com.example.processor.SparkDataConsumer;
import com.example.processor.SparkDataHandler;
import com.example.processor.SparkDataSupplier;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class SparkJobService {

  private final Supplier<Dataset<Row>> supplier;
  private final Function<Dataset<Row>, Dataset<String>> function;
  private final Consumer<Dataset<String>> consumer;

  public SparkJobService(SparkSession spark, SparkProperties sparkProperties) {
    this.supplier = new SparkDataSupplier(spark);
    this.function = new SparkDataHandler();
    this.consumer = new SparkDataConsumer(sparkProperties);
  }

  public void executeJob() {
    consumer.accept(function.apply(supplier.get()));
  }
}
