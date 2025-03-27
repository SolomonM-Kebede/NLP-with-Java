package com.myfirstJavalin.nlp;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import com.myfirstJavalin.config.AppConfig;
import com.myfirstJavalin.database.MongoDBHandler;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import de.tudarmstadt.ukp.dkpro.core.api.ner.type.NamedEntity;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import de.tudarmstadt.ukp.dkpro.core.api.syntax.type.dependency.Dependency;

import org.apache.uima.UIMAFramework;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.CasCreationUtils;
import org.apache.uima.util.XMLInputSource;
import org.apache.uima.util.XMLSerializer;
import org.bson.Document;
import org.hucompute.textimager.uima.type.Sentiment;
import org.hucompute.textimager.uima.type.category.CategoryCoveredTagged;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.uima.type.Topic;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * NLPProcessor handles Natural Language Processing (NLP) tasks,
 * including Named Entity Recognition, Sentiment Analysis, and Dependency Parsing.
 * It integrates MongoDB for data storage and uses UIMA for NLP pipeline execution.
 * Optimized for processing large volumes of documents efficiently.
 */
public class NLPProcessor {
    private static final String XML_DIRECTORY = "/Users/solo/Downloads/uebung4-main/testUebung5/src/main/resources/processedXml";
    private static final Logger logger = LoggerFactory.getLogger(NLPProcessor.class);
    private static final String xmlFilePath = "src/main/resources/newProcessedXml";

    // Configurable NLP Processing Parameters
    private static final int WORKERS_COUNT = 1;
    private static final int BATCH_SIZE = 50;
    private static final int SAVE_BATCH_SIZE = 20;
    private static final int MAX_RETRIES = 3;
    private static final String TYPE_SYSTEM_PATH = "/Users/solo/Downloads/uebung4-main/testUebung5/src/main/resources/TypeSystem.xml";

    private DUUIComposer composer;
    private final Map<String, JCas> listToJCas = new HashMap<>();
    private TypeSystemDescription typeSystemDescription;

    private final AppConfig appConfig;
    private final MongoDBHandler dbConnection;

    /**
     * Initializes the NLPProcessor, setting up database connections,
     * NLP components, and processing pipelines.
     *
     * @throws Exception if initialization fails
     */
    public NLPProcessor() throws Exception {
        this.appConfig = new AppConfig();
        this.dbConnection = new MongoDBHandler();

        // Ensure XML directory exists
        createXmlDirectory();

        // Load type system once
        loadTypeSystem();

        // Initialize NLP components
        initializeComposer();
        initializePipeline();
    }

    /**
     * Creates the XML directory if it doesn't exist
     */
    private void createXmlDirectory() {
        File xmlDir = new File(XML_DIRECTORY);
        if (!xmlDir.exists()) {
            boolean created = xmlDir.mkdirs();
            if (created) {
                logger.info("Created XML directory: {}", XML_DIRECTORY);
            } else {
                logger.warn("Failed to create XML directory: {}", XML_DIRECTORY);
            }
        }
    }

    /**
     * Loads the type system from the external XML file.
     *
     * @throws Exception if loading fails
     */
    private void loadTypeSystem() throws Exception {
        File typeSystemFile = new File(TYPE_SYSTEM_PATH);
        if (!typeSystemFile.exists()) {
            logger.error("Type system file not found: {}", TYPE_SYSTEM_PATH);
            throw new FileNotFoundException("Type system file not found: " + TYPE_SYSTEM_PATH);
        }

        logger.info("Loading type system from: {}", typeSystemFile.getAbsolutePath());
        XMLInputSource inputSource = new XMLInputSource(typeSystemFile);
        typeSystemDescription = UIMAFramework.getXMLParser().parseTypeSystemDescription(inputSource);
        logger.info("Type system loaded successfully");
    }

    /**
     * Initializes the DUUI Composer for NLP processing.
     *
     * @throws Exception if initialization fails
     */
    public void initializeComposer() throws Exception {
        logger.info("Initializing DUUI Composer with {} workers", WORKERS_COUNT);

        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
        this.composer = new DUUIComposer()
                .withSkipVerification(true)
                .withLuaContext(ctx)
                .withWorkers(WORKERS_COUNT);

        DUUIUIMADriver uimaDriver = new DUUIUIMADriver();
        DUUIRemoteDriver remoteDriver = new DUUIRemoteDriver(30000);
        DUUIDockerDriver dockerDriver = new DUUIDockerDriver();

        composer.addDriver(uimaDriver, remoteDriver, dockerDriver);
        logger.info("DUUI Composer initialization complete");
    }

    /**
     * Creates a sample CAS document for testing.
     *
     * @return JCas initialized with a sample text document
     * @throws ResourceInitializationException if CAS initialization fails
     * @throws CASException if CAS operations fail
     */
    public static JCas getCas() throws ResourceInitializationException, CASException {
        // Init a CAS with a static text
        JCas pCas = JCasFactory.createText("Ich finde dieses Programm läuft sehr gut. Ich überlege wie ich dieses für meine Bachelor-Arbeit nachnutzen kann.", "de");

        // Define some metadata to serialize the CAS with the xmi writer
        DocumentMetaData dmd = new DocumentMetaData(pCas);
        dmd.setDocumentId("test");
        dmd.setDocumentTitle("DUUI Test-Dokument");
        dmd.addToIndexes();

        return pCas;
    }

    /**
     * Initializes the NLP pipeline with specific processing components.
     *
     * @throws Exception if pipeline setup fails
     */
    public void initializePipeline() throws Exception {
        composer.resetPipeline();

        // Add remote components with retry logic
        int retries = 0;
        boolean success = false;

        while (!success && retries < MAX_RETRIES) {
            try {
                composer.add(new DUUIRemoteDriver.Component("http://spacy.lehre.texttechnologylab.org")
                        .withScale(WORKERS_COUNT)
                        .build());

                composer.add(new DUUIRemoteDriver.Component("http://gervader.lehre.texttechnologylab.org")
                        .withScale(WORKERS_COUNT)
                        .withParameter("selection", "text")
                        .build());
                // ParlBERT-v2
                composer.add(new DUUIRemoteDriver.Component("http://parlbert.lehre.texttechnologylab.org")
                        .withScale(WORKERS_COUNT)
                        .build());

                // Test the pipeline with a sample document
                JCas testCas = getCas();
                composer.run(testCas);

                success = true;
                logger.info("NLP Pipeline initialized successfully with spaCy and GerVader components");
            } catch (Exception e) {
                retries++;
                logger.warn("Failed to initialize pipeline (attempt {}/{}): {}",
                        retries, MAX_RETRIES, e.getMessage());

                if (retries >= MAX_RETRIES) {
                    throw new Exception("Failed to initialize NLP pipeline after " + MAX_RETRIES + " attempts", e);
                }

                // Wait before retrying
                Thread.sleep(5000);
                composer.resetPipeline();
            }
        }
    }

    /**
     * Processes all unprocessed documents in the database.
     *
     * @throws Exception if processing fails
     */
    public void processAllDocuments() throws Exception {
        MongoCollection<Document> speechCollection = dbConnection.getMongoDatabase()
                .getCollection("speechesCollection");

        // Find total count of unprocessed documents
        long totalCount = speechCollection.countDocuments(
                new Document("processed", new Document("$ne", true))
        );

        logger.info("Found {} unprocessed documents", totalCount);

        if (totalCount == 0) {
            logger.info("No documents to process");
            return;
        }

        // Process in batches
        int totalProcessed = 0;
        long startTime = System.currentTimeMillis();

        while (totalProcessed < totalCount) {
            try {
                // Process a batch
                int batchProcessed = processBatch();

                if (batchProcessed == 0) {
                    logger.warn("No documents processed in this batch, possible end of collection");
                    break;
                }

                totalProcessed += batchProcessed;

                // Calculate progress and ETA
                double percentComplete = (double) totalProcessed / totalCount * 100;
                long elapsedTime = System.currentTimeMillis() - startTime;
                long estimatedTotalTime = (long) (elapsedTime * (totalCount / (double) totalProcessed));
                long remainingTime = estimatedTotalTime - elapsedTime;

                logger.info("Processed {}/{} documents ({:.2f}%) - ETA: {} minutes",
                        totalProcessed, totalCount, percentComplete, remainingTime / 60000);

            } catch (Exception e) {
                logger.error("Error processing batch", e);
            }
        }

        long totalTime = System.currentTimeMillis() - startTime;
        logger.info("Completed processing all documents in {} minutes", totalTime / 60000);
    }

    /**
     * Processes a batch of documents.
     *
     * @return Number of documents processed
     * @throws Exception if processing fails
     */
    private int processBatch() throws Exception {
        // Clear any existing documents
        listToJCas.clear();

        // Load a batch of documents
        int documentsLoaded = loadDocumentBatch();

        if (documentsLoaded == 0) {
            return 0;
        }

        // Process the loaded documents
        processDocuments();

        return documentsLoaded;
    }

    /**
     * Loads a batch of documents from MongoDB into memory.
     * Creates XML files for documents that don't have them yet.
     * Fixed to avoid annotation accumulation.
     *
     * @return Number of documents loaded
     * @throws Exception if loading fails
     */
    private int loadDocumentBatch() throws Exception {
        MongoCollection<Document> speechCollection = dbConnection.getMongoDatabase()
                .getCollection("speechesCollection");

        // Find unprocessed documents
        Document query = new Document("processed", new Document("$ne", true));
        FindIterable<Document> speechDocs = speechCollection.find(query).limit(BATCH_SIZE);

        int loadedCount = 0;

        for (Document speechDoc : speechDocs) {
            String docId = speechDoc.get("_id").toString();
            Path xmlPath = Paths.get(XML_DIRECTORY, docId + ".xmi");

            try {
                JCas jCas;

                // Check if XML file exists
                if (!Files.exists(xmlPath)) {
                    logger.info("XML file not found for document {}. Creating new XML file.", docId);

                    // Create a new JCas from the speech text in MongoDB
                    jCas = createJCasFromSpeech(speechDoc);

                    // Save the JCas as XML - this should contain ONLY text and metadata
                    // (no annotations yet)
                    saveJCasAsXml(jCas, xmlPath.toString());

                    logger.info("Created new XML file for document {}: {}", docId, xmlPath);
                } else {
                    // Read existing XML content from file
                    logger.info("Loading existing XML file for document {}: {}", docId, xmlPath);

                    // IMPORTANT: When loading an existing XML file, we should check if it already has annotations
                    // If it does, we should consider it already processed or create a fresh CAS with just the text
                    String xmlContent = Files.readString(xmlPath, StandardCharsets.UTF_8);

                    // Simple check - if the XML contains annotation markers, create a fresh CAS instead
                    if (xmlContent.contains("<Sentiment")) {
                        logger.warn("XML file for document {} already contains annotations. Creating fresh CAS.", docId);

                        // Extract just the document text from the XML (simplified approach)
                        String docText = extractTextFromXml(xmlContent);
                        String language = extractLanguageFromXml(xmlContent, "de");

                        jCas = JCasFactory.createText(docText, language);

                        // Add minimal metadata
                        DocumentMetaData metadata = new DocumentMetaData(jCas);
                        metadata.setDocumentId(docId);
                        metadata.setLanguage(language);
                        metadata.addToIndexes();

                        // Overwrite the XML with clean version
                        saveJCasAsXml(jCas, xmlPath.toString());
                    } else {
                        // Create CAS and deserialize
                        CAS cas = CasCreationUtils.createCas(typeSystemDescription, null, null, null);
                        byte[] byteList = xmlContent.getBytes(StandardCharsets.UTF_8);

                        try (InputStream input = new ByteArrayInputStream(byteList)) {
                            XmiCasDeserializer.deserialize(input, cas);
                            jCas = cas.getJCas();
                        }
                    }
                }

                // Basic validation
                if (jCas.getDocumentText() == null || jCas.getDocumentText().isEmpty()) {
                    logger.warn("Empty document text for _id: {}", docId);
                    markDocumentAsProcessed(docId, "Empty document text");
                    continue;
                }

                listToJCas.put(docId, jCas);
                loadedCount++;

                if (loadedCount % 10 == 0) {
                    logger.info("Loaded {} documents", loadedCount);
                }
            } catch (Exception e) {
                logger.error("Error loading document {}: {}", docId, e.getMessage());
                markDocumentAsProcessed(docId, "Error: " + e.getMessage());
            }
        }

        logger.info("Loaded {} documents for processing", loadedCount);
        return loadedCount;
    }

    /**
     * Helper method to extract text from an XML string
     */
    private String extractTextFromXml(String xmlContent) {
        // Simple extraction - in a production environment, use proper XML parsing
        int startIdx = xmlContent.indexOf("<cas:sofa");
        if (startIdx > 0) {
            startIdx = xmlContent.indexOf("sofaString=\"", startIdx);
            if (startIdx > 0) {
                startIdx += 12; // Length of 'sofaString="'
                int endIdx = xmlContent.indexOf("\"", startIdx);
                if (endIdx > startIdx) {
                    // Decode XML entities
                    String encodedText = xmlContent.substring(startIdx, endIdx);
                    return encodedText.replace("&lt;", "<")
                            .replace("&gt;", ">")
                            .replace("&amp;", "&")
                            .replace("&quot;", "\"")
                            .replace("&apos;", "'");
                }
            }
        }
        return ""; // Default to empty string if extraction fails
    }

    /**
     * Helper method to extract language from an XML string
     */
    private String extractLanguageFromXml(String xmlContent, String defaultLanguage) {
        int startIdx = xmlContent.indexOf("language=\"");
        if (startIdx > 0) {
            startIdx += 10; // Length of 'language="'
            int endIdx = xmlContent.indexOf("\"", startIdx);
            if (endIdx > startIdx) {
                return xmlContent.substring(startIdx, endIdx);
            }
        }
        return defaultLanguage;
    }

    /**
     * Creates a new JCas from a speech document in MongoDB.
     *
     * @param speechDoc MongoDB document containing speech data
     * @return JCas object with the speech text
     * @throws Exception if creation fails
     */
    private JCas createJCasFromSpeech(Document speechDoc) throws Exception {
        String docId = speechDoc.get("_id").toString();

        // Get text from MongoDB document
        String speechText = speechDoc.getString("text");
        if (speechText == null || speechText.isEmpty()) {
            throw new IllegalArgumentException("Empty or null speech text for document: " + docId);
        }

        // Get language from MongoDB document or default to "de"
        String language = speechDoc.getString("language");
        if (language == null || language.isEmpty()) {
            language = "de"; // Default to German
        }

        // Create a new JCas with the speech text
        JCas jCas = JCasFactory.createText(speechText, language);

        // Add document metadata
        DocumentMetaData metadata = new DocumentMetaData(jCas);
        metadata.setDocumentId(docId);
        metadata.setDocumentTitle(speechDoc.getString("title"));
        metadata.setLanguage(language);

        // Add any additional metadata from MongoDB
        if (speechDoc.containsKey("date")) {
            metadata.setCollectionId(speechDoc.getString("date"));
        }

        // Add metadata to indexes
        metadata.addToIndexes();

        return jCas;
    }

    /**
     * Saves a JCas object as an XML file.
     *
     * @param jCas JCas object to save
     * @param xmlPath Path to save the XML file
     * @throws Exception if saving fails
     */
    private void saveJCasAsXml(JCas jCas, String xmlPath) throws Exception {
        try (FileOutputStream out = new FileOutputStream(xmlPath)) {
            XmiCasSerializer.serialize(jCas.getCas(), out);
        } catch (Exception e) {
            logger.error("Error saving JCas as XML: {}", e.getMessage());
            throw e;
        }
    }

    /**
     * Marks a document as processed with an error message.
     *
     * @param docId Document ID
     * @param errorMessage Error message
     */
    private void markDocumentAsProcessed(String docId, String errorMessage) {
        try {
            MongoCollection<Document> speechCollection = dbConnection.getMongoDatabase()
                    .getCollection("speechesCollection");

            Document query = new Document("_id", docId);
            Document update = new Document("$set", new Document()
                    .append("processed", true)
                    .append("processedAt", new Date())
                    .append("processingError", errorMessage));

            speechCollection.updateOne(query, update);
        } catch (Exception e) {
            logger.error("Failed to mark document {} as processed: {}", docId, e.getMessage());
        }
    }

    /**
     * Processes documents loaded in memory.
     *
     * @throws Exception if processing fails
     */
    public void processDocuments() throws Exception {
        logger.info("Starting NLP document processing for {} documents", listToJCas.size());

        if (listToJCas.isEmpty()) {
            logger.info("No documents to process");
            return;
        }

        // Use bulk operations for efficiency
        List<WriteModel<Document>> bulkWrites = new ArrayList<>();
        AtomicInteger processedCount = new AtomicInteger(0);
        AtomicInteger successCount = new AtomicInteger(0);

        for (Map.Entry<String, JCas> entry : listToJCas.entrySet()) {
            String docId = entry.getKey();
            JCas documentCas = entry.getValue();
            int currentCount = processedCount.incrementAndGet();

            logger.info("Processing document {}/{}: {}", currentCount, listToJCas.size(), docId);

            try {
                // CRITICAL CHANGE: Always create a fresh JCas with just the text and metadata
                // to ensure we don't have annotation accumulation
                JCas freshCas = createFreshJCasFromDocument(documentCas);

                // Run NLP pipeline with retry logic
                boolean pipelineSuccess = runPipelineWithRetry(docId, freshCas);

                if (!pipelineSuccess) {
                    logger.error("Failed to process document {} after retries", docId);
                    markDocumentAsProcessed(docId, "Pipeline processing failed after retries");
                    continue;
                }

                // Update the XML file with the processed JCas
                Path xmlPath = Paths.get(XML_DIRECTORY, docId + ".xmi");
                saveJCasAsXml(freshCas, xmlPath.toString());
                logger.info("Updated XML file with processed annotations: {}", xmlPath);

                // Extract and prepare annotations
                Document annotationsDoc = extractAnnotations(freshCas);

                // Create update document
                Document updateDoc = new Document("$set", annotationsDoc
                        .append("processed", true)
                        .append("processedAt", new Date())
                        .append("processingError", null));

                // Add to bulk write operations
                bulkWrites.add(new UpdateOneModel<>(
                        new Document("_id", docId),
                        updateDoc
                ));

                successCount.incrementAndGet();

                // Execute bulk writes in batches
                if (bulkWrites.size() >= SAVE_BATCH_SIZE) {
                    executeBulkWrites(bulkWrites);
                    bulkWrites.clear();
                }
            } catch (Exception e) {
                logger.error("Error processing document {}: {}", docId, e.getMessage());
                markDocumentAsProcessed(docId, "Error: " + e.getMessage());
            }
        }

        // Execute any remaining bulk writes
        if (!bulkWrites.isEmpty()) {
            executeBulkWrites(bulkWrites);
        }

        logger.info("Completed processing batch: {}/{} documents successful", successCount.get(), processedCount.get());
    }

    /**
     * Creates a fresh JCas with only text and metadata from an existing JCas
     * This prevents annotation accumulation during retries
     *
     * @param sourceCas Source JCas to copy from
     * @return Fresh JCas with only text and metadata
     */
    private JCas createFreshJCasFromDocument(JCas sourceCas) throws Exception {
        // Create a new JCas with the same text and language
        JCas freshCas = JCasFactory.createText(sourceCas.getDocumentText(), sourceCas.getDocumentLanguage());

        // Copy document metadata if it exists
        try {
            DocumentMetaData sourceMetadata = DocumentMetaData.get(sourceCas);
            if (sourceMetadata != null) {
                DocumentMetaData newMetadata = new DocumentMetaData(freshCas);
                newMetadata.setDocumentId(sourceMetadata.getDocumentId());
                newMetadata.setDocumentTitle(sourceMetadata.getDocumentTitle());
                newMetadata.setLanguage(sourceMetadata.getLanguage());
                if (sourceMetadata.getCollectionId() != null) {
                    newMetadata.setCollectionId(sourceMetadata.getCollectionId());
                }
                newMetadata.addToIndexes();
            }
        } catch (IllegalArgumentException e) {
            // If no metadata exists, create basic metadata
            DocumentMetaData newMetadata = new DocumentMetaData(freshCas);
            newMetadata.setDocumentId(UUID.randomUUID().toString());
            newMetadata.setLanguage(sourceCas.getDocumentLanguage());
            newMetadata.addToIndexes();
        }

        return freshCas;
    }

    /**
     * Runs the NLP pipeline with retry logic.
     *
     * @param docId Document ID
     * @param documentCas The document CAS to process
     * @return True if processing succeeded, false otherwise
     */
    private boolean runPipelineWithRetry(String docId, JCas documentCas) {
        int retries = 0;
        boolean success = false;

        while (retries < MAX_RETRIES && !success) {
            try {
                // Run the pipeline on the CAS
                // No need for a fresh CAS on each retry - we already have a clean one from createFreshJCasFromDocument
                composer.run(documentCas);

                // Update the reference in the map with the processed document
                listToJCas.put(docId, documentCas);

                success = true;
            } catch (Exception e) {
                retries++;
                logger.warn("Pipeline execution failed (attempt {}/{}): {}",
                        retries, MAX_RETRIES, e.getMessage());
                if (retries >= MAX_RETRIES) {
                    logger.error("Pipeline failed after {} retries: {}", MAX_RETRIES, e.getMessage());
                    return false;
                }
                try { Thread.sleep(5000); } catch (InterruptedException ie) { /* Handle */ }
            }
        }
        return success;
    }

    private JCas reloadJCasFromXml(String docId) throws Exception {
        Path xmlPath = Paths.get(XML_DIRECTORY, docId + ".xmi");
        String xmlContent = Files.readString(xmlPath, StandardCharsets.UTF_8);

        CAS cas = CasCreationUtils.createCas(typeSystemDescription, null, null, null);
        try (InputStream input = new ByteArrayInputStream(xmlContent.getBytes(StandardCharsets.UTF_8))) {
            XmiCasDeserializer.deserialize(input, cas);
            return cas.getJCas();
        } catch (Exception e) {
            logger.error("Failed to reload JCas for document {}: {}", docId, e.getMessage());
            throw e;
        }
    }

    /**
     * Executes bulk write operations to MongoDB.
     *
     * @param bulkWrites List of write operations
     */
    private void executeBulkWrites(List<WriteModel<Document>> bulkWrites) {
        if (bulkWrites.isEmpty()) {
            return;
        }

        MongoCollection<Document> speechCollection = dbConnection.getMongoDatabase()
                .getCollection("speechesCollection");

        try {
            BulkWriteResult result = speechCollection.bulkWrite(bulkWrites, new BulkWriteOptions().ordered(false));
            logger.info("Bulk write completed: {} documents modified", result.getModifiedCount());
        } catch (Exception e) {
            logger.error("Bulk write failed: {}", e.getMessage());
        }
    }

    /**
     * Extracts all annotations from a document CAS.
     * New implementation that properly extracts only valid annotations.
     *
     * @param jCas The document CAS
     * @return Document containing all annotations
     */
    private Document extractAnnotations(JCas jCas) {
        // Extract all annotations in a single pass
        List<Document> tokenDocs = new ArrayList<>();
        List<Document> sentenceDocs = new ArrayList<>();
        List<Document> namedEntityDocs = new ArrayList<>();
        List<Document> dependencyDocs = new ArrayList<>();
        List<Document> sentimentDocs = new ArrayList<>();
        List<Document> topicsList = new ArrayList<>();
        Map<String, CategoryCoveredTagged> highestScoreTopic = new HashMap<>();

        // Process sentences and contained annotations
        double sentimentSum = 0.0;
        int sentimentCount = 0;

        // Create a set to track which annotations we've already processed
        // to prevent duplicate processing
        Set<Integer> processedSentiments = new HashSet<>();
        Set<Integer> processedEntities = new HashSet<>();
        Set<Integer> processedDependencies = new HashSet<>();

        for (Sentence sentence : JCasUtil.select(jCas, Sentence.class)) {
            // Process sentence
            Document sentenceDoc = new Document()
                    .append("text", sentence.getCoveredText())
                    .append("begin", sentence.getBegin())
                    .append("end", sentence.getEnd());

            // Process tokens in this sentence
            List<Token> tokens = JCasUtil.selectCovered(Token.class, sentence);
            sentenceDoc.append("tokenCount", tokens.size());
            sentenceDocs.add(sentenceDoc);

            for (Token t : tokens) {
                Document tokenDoc = new Document()
                        .append("lemmaValue", t.getLemmaValue())
                        .append("pos", t.getPosValue())
                        .append("text", t.getText())
                        .append("begin", t.getBegin())
                        .append("end", t.getEnd());
                tokenDocs.add(tokenDoc);
            }

            // Process dependencies in this sentence using a tracking set to prevent duplicates
            for (Dependency dep : JCasUtil.selectCovered(Dependency.class, sentence)) {
                int depHash = Objects.hash(dep.getBegin(), dep.getEnd(), dep.getDependencyType());
                if (!processedDependencies.contains(depHash) && dep.getDependencyType() != null) {
                    processedDependencies.add(depHash);

                    Document dependencyDoc = new Document()
                            .append("dependencyType", dep.getDependencyType())
                            .append("text", dep.getCoveredText())
                            .append("begin", dep.getBegin())
                            .append("end", dep.getEnd());
                    dependencyDocs.add(dependencyDoc);
                }
            }

            // Process sentiment in this sentence using a tracking set to prevent duplicates
            for (Sentiment sentiment : JCasUtil.selectCovered(Sentiment.class, sentence)) {
                int sentHash = Objects.hash(sentiment.getBegin(), sentiment.getEnd());
                if (!processedSentiments.contains(sentHash)) {
                    processedSentiments.add(sentHash);

                    double sentimentValue = sentiment.getSentiment();
                    sentimentSum += sentimentValue;
                    sentimentCount++;

                    Document sentimentDoc = new Document()
                            .append("sentiment_value", sentimentValue)
                            .append("sentence", sentiment.getCoveredText())
                            .append("begin", sentiment.getBegin())
                            .append("end", sentiment.getEnd());
                    sentimentDocs.add(sentimentDoc);
                }
            }
        }

        // Process named entities using a tracking set to prevent duplicates
        for (NamedEntity ne : JCasUtil.select(jCas, NamedEntity.class)) {
            int neHash = Objects.hash(ne.getBegin(), ne.getEnd(), ne.getValue());
            if (!processedEntities.contains(neHash) && ne.getValue() != null) {
                processedEntities.add(neHash);

                Document neDoc = new Document()
                        .append("value", ne.getValue())
                        .append("text", ne.getCoveredText())
                        .append("begin", ne.getBegin())
                        .append("end", ne.getEnd());
                namedEntityDocs.add(neDoc);
            }
        }

        // Process topic annotations
        List<String> topics = new ArrayList<>();

        JCasUtil.select(jCas, CategoryCoveredTagged.class).forEach(topic -> {
            if (topic.getValue() != null) {
                String sentence = topic.getBegin() + "-" + topic.getEnd();

                if (!highestScoreTopic.containsKey(sentence) ||
                        topic.getScore() > highestScoreTopic.get(sentence).getScore()) {
                    highestScoreTopic.put(sentence, topic);
                }
            }
        });

        for (CategoryCoveredTagged highestTopic : highestScoreTopic.values()) {
            topics.add(highestTopic.getValue());

            Document topicDoc = new Document("topic", highestTopic.getValue())
                    .append("score", highestTopic.getScore())
                    .append("begin", highestTopic.getBegin())
                    .append("end", highestTopic.getEnd());
            topicsList.add(topicDoc);
        }

        // Calculate average sentiment
        double averageSentiment = sentimentCount > 0 ? sentimentSum / sentimentCount : 0.0;

        // Create a document with all annotations
        Document result = new Document()
                .append("token", tokenDocs)
                .append("sentence", sentenceDocs)
                .append("namedEntities", namedEntityDocs)
                .append("dependency", dependencyDocs)
                .append("sentiment", sentimentDocs)
                .append("average_sentiment", averageSentiment)
                .append("topics", topicsList);
        //.append("topic_list", topics);

        logger.info("Extracted counts - Sentences: {}, Tokens: {}, NEs: {}, Dependencies: {}, Sentiments: {}, Topics: {}",
                sentenceDocs.size(), tokenDocs.size(), namedEntityDocs.size(),
                dependencyDocs.size(), sentimentDocs.size(), topicsList.size());

        return result;
    }

    /**
     * Main method to run the NLP processor.
     *
     * @param args Command line arguments
     */
    public static void main(String[] args) {
        try {
            // Normal processing
            logger.info("Starting NLP processor");
            NLPProcessor nlpProcessor = new NLPProcessor();
            nlpProcessor.processAllDocuments();
            logger.info("NLP processor completed successfully");
        } catch (Exception e) {
            logger.error("Critical error in NLP processing", e);
        }
    }
}