package com.example.processor;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import java.util.function.Function;

public class SparkDataHandler implements Function<Dataset<Row>, Dataset<String>> {

  @Override
  public Dataset<String> apply(Dataset<Row> dataset) {
    return dataset.as(Encoders.STRING());
  }
}
