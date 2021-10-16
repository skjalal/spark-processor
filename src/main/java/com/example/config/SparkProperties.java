package com.example.config;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.Properties;

@Slf4j
public class SparkProperties {

  private static final String APPLICATION_PROPERTIES = "application.properties";
  private final Properties properties;

  public SparkProperties(Properties prop) {
    this.properties = prop;
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    try (InputStream stream = loader.getResourceAsStream(APPLICATION_PROPERTIES)) {
      properties.load(stream);
    } catch (IOException e) {
      log.error("Failed to Load Properties", e);
    }
  }

  public String getPropertyValue(String key) {
    return Optional.ofNullable(properties.getProperty(key)).orElseThrow(() -> new IllegalArgumentException("Resource Not Found"));
  }
}
