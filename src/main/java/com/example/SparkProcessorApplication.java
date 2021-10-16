package com.example;

import com.example.config.SparkConfiguration;
import com.example.config.SparkProperties;
import com.example.service.SparkJobService;

import java.util.Properties;

public class SparkProcessorApplication {

  public static void main(String[] args) {
    SparkProperties sparkProperties = new SparkProperties(new Properties());
    SparkConfiguration configuration = new SparkConfiguration(sparkProperties);
    new SparkJobService(configuration.getSparkSession(), sparkProperties).executeJob();
    configuration.closeSparkSession();
  }
}
