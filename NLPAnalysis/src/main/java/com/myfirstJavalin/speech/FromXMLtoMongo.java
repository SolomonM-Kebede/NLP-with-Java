package com.myfirstJavalin.speech;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.WriteModel;
import com.myfirstJavalin.config.AppConfig;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class parses XML speech protocol files and inserts the extracted data into a MongoDB database.
 * It uses SAX parsing to process XML files and updates the MongoDB collections accordingly.
 *
 * @author Solomon Mengesha Kebede
 */
public class FromXMLtoMongo {

    /**
     * The main entry point for parsing XML files and storing the extracted data in MongoDB.
     *
     * @param args Command-line arguments (not used).
     * @throws IOException If an I/O error occurs while accessing the XML files.
     */
    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(FromXMLtoMongo.class);
        String folderPath = "protocols";
        AppConfig appConfig = new AppConfig();

        try {
            var mongoClient = MongoClients.create(appConfig.getMongoUri());
            MongoDatabase database = mongoClient.getDatabase(appConfig.getMongoDatabase());
            MongoCollection<Document> speechCollection = database.getCollection("speeches");
            MongoCollection<Document> agendaCollection = database.getCollection("agenda");
            MongoCollection<Document> commentCollection = database.getCollection("comments");

            SAXParserFactory factory = SAXParserFactory.newInstance();

            File folder = new File(folderPath);
            File[] xmlFiles = folder.listFiles((dir, name) -> name.endsWith(".xml"));
            if (xmlFiles == null || xmlFiles.length == 0) {
                System.out.println("No XML-Data.");
                return;
            }

            // Concurrent collections to store parsed documents
            ConcurrentLinkedQueue<Document> allSpeeches = new ConcurrentLinkedQueue<>();
            ConcurrentLinkedQueue<Document> allAgendas = new ConcurrentLinkedQueue<>();
            ConcurrentLinkedQueue<Document> allComments = new ConcurrentLinkedQueue<>();

            AtomicInteger totalProcessedFiles = new AtomicInteger(0);
            int totalFiles = xmlFiles.length;

            // Create thread pool - adjust the number based on CPU cores
            int processors = Runtime.getRuntime().availableProcessors();
            ExecutorService executor = Executors.newFixedThreadPool(processors);

            // Submit parsing tasks
            for (File file : xmlFiles) {
                executor.submit(() -> {
                    try {
                        SAXParser saxParser = factory.newSAXParser();
                        SpeechHandler handler = new SpeechHandler();
                        saxParser.parse(file, handler);

                        // Add all parsed documents to our concurrent collections
                        allSpeeches.addAll(handler.getAllSpeeches());
                        allAgendas.addAll(handler.getAllAgendaItems());
                        allComments.addAll(handler.getAllComments());

                        int processed = totalProcessedFiles.incrementAndGet();
                        System.out.println("Processed file " + processed + "/" + totalFiles +
                                ": " + file.getName());
                    } catch (Exception e) {
                        logger.error("Error processing " + file.getName() + ": " + e.getMessage(), e);
                    }
                });
            }

            // Shutdown the executor and wait for all tasks to complete
            executor.shutdown();
            boolean finished = executor.awaitTermination(1, TimeUnit.HOURS);

            if (finished) {
                System.out.println("All parsing tasks completed. Saving to MongoDB...");

                // Convert to lists for bulk operations
                List<Document> speechList = new ArrayList<>(allSpeeches);
                List<Document> agendaList = new ArrayList<>(allAgendas);
                List<Document> commentList = new ArrayList<>(allComments);

                // Save in batches
                int speechCount = saveBulkDocuments(speechCollection, speechList);
                int agendaCount = saveBulkDocuments(agendaCollection, agendaList);
                int commentCount = saveBulkDocuments(commentCollection, commentList);

                System.out.println("Saved " + speechCount + " speeches, " +
                        agendaCount + " agenda items, and " +
                        commentCount + " comments.");
            } else {
                System.out.println("Processing timed out after 1 hour.");
            }

        } catch (Exception e) {
            logger.error("Fatal error: " + e.getMessage(), e);
        }
    }

    private static int saveBulkDocuments(MongoCollection<Document> collection, List<Document> documents) {
        if (documents.isEmpty()) {
            return 0;
        }

        int batchSize = 1000;
        int saved = 0;

        for (int i = 0; i < documents.size(); i += batchSize) {
            List<WriteModel<Document>> bulkOperations = new ArrayList<>();

            int end = Math.min(i + batchSize, documents.size());
            for (int j = i; j < end; j++) {
                Document doc = documents.get(j);
                bulkOperations.add(new ReplaceOneModel<>(
                        new Document("_id", doc.get("_id")),
                        doc,
                        new com.mongodb.client.model.ReplaceOptions().upsert(true)
                ));
            }

            if (!bulkOperations.isEmpty()) {
                collection.bulkWrite(bulkOperations, new BulkWriteOptions().ordered(false));
                saved += bulkOperations.size();
            }
        }

        return saved;
    }
}
