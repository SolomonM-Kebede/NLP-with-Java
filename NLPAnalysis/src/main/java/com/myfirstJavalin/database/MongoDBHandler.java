package com.myfirstJavalin.database;

import com.mongodb.client.*;
import com.mongodb.client.model.*;
import com.mongodb.client.result.InsertOneResult;
import com.myfirstJavalin.config.AppConfig;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.ILoggerFactory;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MongoDBHandler {
    private static final Logger LOGGER = Logger.getLogger(MongoDBHandler.class.getName());
    private final org.slf4j.Logger logger = LoggerFactory.getLogger(MongoDBHandler.class);

    private final MongoClient mongoClient;
    private final MongoDatabase database;
    private final String defaultCollectionName;
    private MongoDatabase mongoDatabase;
    AppConfig config = new AppConfig();

    public MongoDBHandler() throws IOException {
        Logger.getLogger("org.mongodb.driver").setLevel(Level.WARNING);


        this.mongoClient = MongoClients.create(config.getMongoUri());
        this.database = mongoClient.getDatabase(config.getMongoDatabase());
        this.defaultCollectionName = config.getAbgeordneterCollection();

        LOGGER.info("Connected to MongoDB: " + database.getName());
    }

    // Core Collection Operations
    public void createCollectionIfMissing(String collectionName) {
        try {
            if (!collectionExists(collectionName)) {
                database.createCollection(collectionName);
                LOGGER.info("Created collection: " + collectionName);
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Collection creation failed", e);
        }
    }

    public boolean collectionExists(String collectionName) {
        try {
            return database.listCollectionNames()
                    .into(new ArrayList<>())
                    .contains(collectionName);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Collection check failed", e);
            return false;
        }
    }

    // Index Management
    public void createIndex(String collectionName, String fieldName) {
        createIndex(collectionName, Indexes.ascending(fieldName));
    }

    public void createIndex(String collectionName, Bson index) {
        try {
            database.getCollection(collectionName)
                    .createIndex(index);
            LOGGER.info("Created index on " + index + " in " + collectionName);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Index creation failed", e);
        }
    }

    // Document Operations
    public void saveData(String collectionName, Document document) {
        try {
            InsertOneResult result = database.getCollection(collectionName)
                    .insertOne(document);
            LOGGER.fine("Inserted document ID: " + result.getInsertedId());
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Document insertion failed", e);
        }
    }

    public void saveBulkData(String collectionName, List<Document> documents) {
        try {
            database.getCollection(collectionName)
                    .insertMany(documents);
            LOGGER.info("Inserted " + documents.size() + " documents");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Bulk insert failed", e);
        }
    }

    // Query Operations
    public boolean documentExists(String collectionName, Bson filter) {
        try {
            return database.getCollection(collectionName)
                    .countDocuments(filter) > 0;
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Document check failed", e);
            return false;
        }
    }

    public List<Document> getAllDocuments(String collectionName) {
        List<Document> documents = new ArrayList<>();
        try {
            database.getCollection(collectionName)
                    .find()
                    .forEach(documents::add);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Document retrieval failed", e);
        }
        return documents;
    }

    // Connection Management
    public void close() {
        try {
            if (mongoClient != null) {
                mongoClient.close();
                LOGGER.info("MongoDB connection closed");
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Connection close failed", e);
        }
    }

    // Additional Methods for Debate Scraper
    public Set<String> getExistingSessionIds() {
        return getFieldValues("sessions", "_id");
    }

    public Set<String> getFieldValues(String collectionName, String field) {
        Set<String> values = new HashSet<>();
        try {
            database.getCollection(collectionName)
                    .distinct(field, String.class)
                    .forEach(values::add);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Field value retrieval failed", e);
        }
        return values;
    }

    public void createTextIndex(String collectionName, String... fields) {
        try {
            database.getCollection(collectionName)
                    .createIndex(Indexes.text(Arrays.toString(fields)));
            LOGGER.info("Created text index on " + Arrays.toString(fields));
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Text index creation failed", e);
        }
    }


    public MongoDatabase getMongoDatabase() {
        try {
            // Log the database name you're trying to access
            logger.info("Attempting to get database: {}",config.getMongoDatabase());
            MongoDatabase db = mongoClient.getDatabase(config.getMongoDatabase());

            // Test the connection with a simple command
            db.runCommand(new Document("ping", 1));
            logger.info("Successfully connected to database: {}", config.getMongoDatabase());

            return db;
        } catch (Exception e) {
            logger.error("Error retrieving MongoDB database: {}", e.getMessage(), e);
            return null;
        }
    }
    public void closeConnection() {
        mongoClient.close();
    }

    public List<String> getAllSpeechIds() {
        return null;
    }
}