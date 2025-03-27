package com.myfirstJavalin.speech;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.myfirstJavalin.config.AppConfig;
import org.bson.Document;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.File;
import java.io.IOException;
import java.util.List;

public class BatchXMLtoMongoDB {
    public static void main(String[] args) throws IOException {
        String folderPath = "/Users/solo/Downloads/uebung4-main/testUebung5/protocols";
        AppConfig appConfig = new AppConfig();

        try {
            var mongoClient = MongoClients.create(appConfig.getMongoUri());
            MongoDatabase database = mongoClient.getDatabase(appConfig.getMongoDatabase());
            MongoCollection<Document> speechCollection = database.getCollection("speechesCollection");
            //MongoCollection<Document> agendaCollection = database.getCollection("agenda");

            SAXParserFactory factory = SAXParserFactory.newInstance();
            SAXParser saxParser = factory.newSAXParser();

            File folder = new File(folderPath);
            File[] xmlFiles = folder.listFiles((dir, name) -> name.endsWith(".xml"));
            if (xmlFiles == null || xmlFiles.length == 0) {
                System.out.println("No XML-Data.");
                return;
            }

            int totalSpeeches = 0, totalAgenda = 0;
            for (File file : xmlFiles) {
                try {
                    System.out.println("Processing: " + file.getName());
                    SpeechSAXHandler handler = new SpeechSAXHandler();
                    saxParser.parse(file, handler);

                    List<Document> speechDocuments = handler.getAllSpeeches();
                    List<Document> agendaDocuments = handler.getAllAgendaItems();

                    for (Document speechDocument : speechDocuments) {
                        speechCollection.replaceOne(
                                new Document("_id", speechDocument.get("_id")),
                                speechDocument,
                                new com.mongodb.client.model.ReplaceOptions().upsert(true)
                        );
                        totalSpeeches++;
                    }

                    /*for (Document agendaDocument : agendaDocuments) {
                        agendaCollection.replaceOne(
                                new Document("_id", agendaDocument.get("_id")),
                                agendaDocument,
                                new com.mongodb.client.model.ReplaceOptions().upsert(true)
                        );
                        totalAgenda++;
                    }*/
                } catch (SAXException | IOException e) {
                    System.err.println("Error finding  " + file.getName() + ": " + e.getMessage());
                }
            }

            System.out.println("Batch processing is closed: " + totalSpeeches + " speech and " + totalAgenda + " Agenda saved.");
        } catch (Exception e) {
            LoggerFactory.getLogger(BatchXMLtoMongoDB.class).error(e.getMessage(), e);
        }
    }
}