package com.myfirstJavalin.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class AppConfig {
    private final Properties properties;

    public AppConfig() throws IOException {
        properties = new Properties();
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("config.properties")) {
            if (input == null) {
                throw new IOException("config.properties not found in classpath");
            }
            properties.load(input);
        }
        validateRequiredProperties();
    }

    private void validateRequiredProperties() {
        String[] required = {
                "mongo.uri", "mongo.database", "mongo.username", "mongo.password",
                "mongo.collection.abgeordneter", "mongo.collection.speech",
                "bundestag.base.url", "mediathek.url", "thread.pool.size"
        };

        for (String key : required) {
            if (!properties.containsKey(key)) {
                throw new RuntimeException("Missing required property: " + key);
            }
        }
    }

    // MongoDB configuration
    public String getMongoUri() {
        return properties.getProperty("mongo.uri").trim();
    }

    public String getMongoDatabase() {
        return properties.getProperty("mongo.database").trim();
    }

    public String getMongoUsername() {
        return properties.getProperty("mongo.username").trim();
    }

    public String getMongoPassword() {
        return properties.getProperty("mongo.password").trim();
    }

    // Collection names
    public String getAbgeordneterCollection() {
        return properties.getProperty("mongo.collection.abgeordneter").trim();
    }

    public String getSpeechCollection() {
        return properties.getProperty("mongo.collection.speech").trim();
    }

    // URLs
    public String getBundestagBaseUrl() {
        return properties.getProperty("bundestag.base.url").trim();
    }

    public String getMediathekUrl() {
        return properties.getProperty("mediathek.url").trim();
    }

    // Thread configuration
    public int getThreadPoolSize() {
        return Integer.parseInt(properties.getProperty("thread.pool.size").trim());
    }

    // Application configuration
    public String getBundestagUrl() { return "https://www.bundestag.de/services/opendata"; }
    public String getDownloadDir() { return "./protocols"; }
    public long getUpdateInterval() { return 24 * 60 * 60 * 1000; } // 24 hours
    public String getProperty(String key) {
        return properties.getProperty(key);
    }
    public String getProperty(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }
}