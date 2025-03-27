package com.myfirstJavalin.nlp;


import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import com.myfirstJavalin.config.AppConfig;
import com.myfirstJavalin.database.MongoDBHandler;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.io.FilenameUtils;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.uima.UIMAException;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.util.InvalidXMLException;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.xml.sax.SAXException;


public class VideoNLPProcessor {
    private static final Logger logger = LoggerFactory.getLogger(VideoNLPProcessor.class);
    private static final String VIDEO_VIEW = "video";
    private static final String TRANSCRIPT_VIEW = "transcript";
    private static final int SPEECH_ID_LENGTH = 12;
    private static final int WORKERS = 1;
    private DUUIComposer composer;
    private final MongoDBHandler dbConnection;

    /**
     * Class to extract transcriptions with timestamps from video speech and store in MongoDB
     * @param dbConnection connect with MongoDB
     * @throws Exception throws exception
     */
    public VideoNLPProcessor(MongoDBHandler dbConnection) throws Exception {
        this.dbConnection = dbConnection;
        this.initializeComposer();
        this.initializePipeline();
    }


    private void initializeComposer() throws IOException, UIMAException, SAXException, URISyntaxException {
        this.composer = (new DUUIComposer())
                .withSkipVerification(true)
                .withLuaContext((new DUUILuaContext())
                        .withJsonLibrary()).withWorkers(1)
                .addDriver(new DUUIUIMADriver())
                .addDriver(new DUUIDockerDriver())
                .addDriver(new DUUIRemoteDriver());
    }

    /**
     * Initializes the pipeline that processes the speech with WhisperX
     * @throws URISyntaxException syntax exception
     * @throws IOException input output exception
     * @throws CompressorException compressor exception
     * @throws InvalidXMLException InvalidXML exception
     * @throws SAXException SAX exception
     */
    private void initializePipeline() throws URISyntaxException, IOException, CompressorException, InvalidXMLException, SAXException {
        this.composer.resetPipeline();
        this.composer.add((new DUUIRemoteDriver.Component("http://whisperx.lehre.texttechnologylab.org"))
                .withScale(1)
                .withSourceView("video")
                .withTargetView("transcript")
                .withParameter("return_timestamps", "true")
                .withParameter("word_timestamps", "true").build());
    }

    /**
     * Process speech from the video
     * @param videoPath the local path of mp4 video
     * @throws Exception throws exception
     * @return the speechId used for processing
     */
    public String processSpeech(Path videoPath) throws Exception {
        String filename = videoPath.getFileName().toString();
        String baseFilename = FilenameUtils.removeExtension(filename);
        String speechId = this.generateSpeechId(baseFilename);
        JCas mainCas = JCasFactory.createJCas();

        String var7;
        try {
            JCas videoView = this.createVideoView(mainCas, speechId, videoPath);
            this.composer.run(videoView);
            this.saveToMongoDB(mainCas, speechId);
            var7 = speechId;
        } finally {
            mainCas.release();
        }

        return var7;
    }

    /**
     * Generate a speechId from filename by taking first 12 characters
     * @param filename the filename without extension
     * @return the generated speechId
     */
    private String generateSpeechId(String filename) {
        return filename.length() <= 12 ? filename : filename.substring(0, 12);
    }

    /**
     * it creates video view by converting to bas64video
     * @param mainCas mainCas
     * @param speechId speech id of the speech
     * @param videoPath the path of the video
     * @return return video view
     * @throws Exception throws exception
     */
    private JCas createVideoView(JCas mainCas, String speechId, Path videoPath) throws Exception {
        JCas videoView = mainCas.createView("video");

        byte[] videoBytes;
        try {
            if (!Files.exists(videoPath, new LinkOption[0])) {
                throw new IOException("Video file not found: " + String.valueOf(videoPath));
            }

            videoBytes = Files.readAllBytes(videoPath);
        } catch (IOException e) {
            logger.error("Failed to read video file: {}", videoPath, e);
            throw new CASException("Video file read error", new Object[]{e});
        }

        String base64Video = Base64.getEncoder().encodeToString(videoBytes);
        DocumentMetaData meta = DocumentMetaData.create(videoView);
        meta.setDocumentId(speechId);
        meta.setDocumentTitle("Video processing for speech: " + speechId);
        videoView.setSofaDataString(base64Video, "text/plain");
        videoView.setDocumentLanguage("de");
        return videoView;
    }

    /**
     * Responsible to save processed data to MongoDB
     * @param mainCas main cas
     * @param speechId speech id
     * @throws CASException cas exception
     */
    private void saveToMongoDB(JCas mainCas, String speechId) throws CASException {
        MongoDatabase database = this.dbConnection.getMongoDatabase();
        GridFSBucket gridFSBucket = GridFSBuckets.create(database, "video1");
        MongoCollection<Document> collection = database.getCollection("speechTranscripts");

        try {
            Document metadata = this.extractVideoMetadata(mainCas);
            ObjectId videoId = this.storeVideoInGridFS(mainCas, gridFSBucket, speechId);
            Document transcriptWithTimestamps = this.extractTranscriptWithTimestamps(mainCas, speechId);
            UpdateResult result = collection.updateOne(new Document("_id", speechId),
                    (new Document("$set", (new Document())
                            .append("metadata", metadata)
                            .append("video_ref", videoId.toString())
                            .append("transcript", transcriptWithTimestamps)))
                            .append("$setOnInsert", new Document("_id", speechId)), (new UpdateOptions())
                            .upsert(true));
            logger.info("Matched {} document(s), modified {} document(s)", result.getMatchedCount(), result.getModifiedCount());
        } catch (IOException e) {
            throw new CASException("Error storing data in GridFS", new Object[]{e});
        }
    }

    // helper methods for GridFS storage
    private ObjectId storeVideoInGridFS(JCas mainCas, GridFSBucket gridFSBucket, String speechId) throws IOException, CASException {
        JCas videoView = mainCas.getView("video");
        String base64Data = videoView.getSofaDataString();

        try {
            byte[] videoData = Base64.getDecoder().decode(base64Data);

            try (InputStream stream = new ByteArrayInputStream(videoData)) {
                return gridFSBucket.uploadFromStream(speechId + ".mp4", stream);
            }
        } catch (IllegalArgumentException e) {
            logger.error("Invalid Base64 data for speech ID: {}", speechId, e);
            throw new CASException("Invalid video data encoding", new Object[]{e});
        }
    }

    // Extract video metadata
    private Document extractVideoMetadata(JCas mainCas) throws CASException {
        JCas videoView = mainCas.getView("video");
        return (new Document())
                .append("processing_date", System.currentTimeMillis())
                .append("mime_type", videoView.getSofaMimeType())
                .append("language", videoView.getDocumentLanguage());
    }

    /**
     * Extract transcript with timestamps and sentence-level segmentation
     * @param mainCas the main CAS object
     * @param speechId the speech ID
     * @return Document containing transcript with timestamps and sentence segments
     * @throws CASException if there's an issue with CAS processing
     */
    private Document extractTranscriptWithTimestamps(JCas mainCas, String speechId) throws CASException {
        JCas transcriptView = mainCas.getView("transcript");
        String transcriptData = transcriptView.getSofaDataString();
        String fullText = transcriptView.getDocumentText();
        logger.debug("Raw WhisperX output (first 500 chars): {}", transcriptData.substring(0, Math.min(500, transcriptData.length())));

        try {
            Document timestampInfo = this.tryExtractTimestamps(transcriptData, fullText);
            List<Document> sentences = this.segmentTextIntoSentences(fullText);
            return (new Document())
                    .append("t_Id", "transcript-" + speechId)
                    .append("transcriptText", fullText)
                    .append("sentences", sentences)
                    .append("segments_with_timeStamps", timestampInfo.get("segments_with_timeStamps"));
        } catch (Exception e) {
            logger.warn("Failed to parse transcript: {}", e.getMessage(), e);
            List<Document> sentences = this.segmentTextIntoSentences(fullText);
            return (new Document())
                    .append("t_Id", "transcript-" + speechId)
                    .append("transcriptText", fullText)
                    .append("sentences", sentences)
                    .append("parsing_error", e.getMessage());
        }
    }

    /**
     * Segment text into sentences with begin/end positions
     */
    private List<Document> segmentTextIntoSentences(String text) {
        List<Document> sentences = new ArrayList();
        Pattern sentencePattern = Pattern.compile("([^.!?\\s][^.!?]*(?:[.!?](?!['\"]?\\s|$)[^.!?]*)*[.!?]?['\"]?(?=\\s|$))", 8);
        Matcher matcher = sentencePattern.matcher(text);

        while(matcher.find()) {
            String sentence = matcher.group().trim();
            int begin = matcher.start();
            int end = matcher.end();
            sentences.add((new Document()).append("text", sentence).append("begin", begin).append("end", end));
        }

        // If no sentences were found (regex failed), use a simple fallback approach
        if (sentences.isEmpty()) {
            String[] simpleSentences = text.split("(?<=[.!?])\\s+");
            int position = 0;

            for(String sentence : simpleSentences) {
                if (!sentence.trim().isEmpty()) {
                    int end = position + sentence.length();
                    sentences.add((new Document())
                            .append("text", sentence.trim())
                            .append("begin", position)
                            .append("end", end));
                    position = end + 1;
                }
            }
        }

        return sentences;
    }

    /**
     * Try multiple approaches to extract timestamp information
     */
    private Document tryExtractTimestamps(String transcriptData, String fullText) {
        // Try different parsing strategies in order of preference
        try {
            //  try JSON parsing
            return this.parseJsonTranscript(transcriptData);
        } catch (Exception var8) {
            try {
                // Try SRT/WebVTT format if it doesnt start with {
                if (transcriptData.contains("-->")) {
                    return this.parseSRTOrVTTFormat(transcriptData, fullText);
                }
            } catch (Exception srtEx) {
                logger.info("SRT/VTT parsing failed: {}", srtEx.getMessage());
            }

            try {
                // Try speaker timestamp format
                if (transcriptData.contains("[") && transcriptData.contains("]")) {
                    return this.parseSpeakerTimestampFormat(transcriptData, fullText);
                }
            } catch (Exception spkEx) {
                logger.info("Speaker timestamp parsing failed: {}", spkEx.getMessage());
            }

            try {
                // Try line-by-line timestamps
                if (transcriptData.matches("(?s).*\\d+:\\d+:\\d+.*\\n.*")) {
                    return this.parseLineByLineTimestamps(transcriptData, fullText);
                }
            } catch (Exception lineEx) {
                logger.info("Line timestamp parsing failed: {}", lineEx.getMessage());
            }

            // Final fallback - generate estimated timestamps
            return this.generateEstimatedTimestamps(fullText);
        }
    }

    /**
     * Generate estimated timestamps when no actual timing information is available
     */
    private Document generateEstimatedTimestamps(String fullText) {
        List<Document> segments = new ArrayList<>();
        // Split text into approximately equal chunks as a fallback
        // Calculate a rough estimated speaking rate
        double wordsPerSecond = (double)2.5F;
        String[] words = fullText.split("\\s+");
        int totalWords = words.length;
        // Create segments of roughly 10-15 words
        int segmentSize = 12;

        double currentTime = (double)0.0F;
        StringBuilder segmentText = new StringBuilder();
        int wordCount = 0;
        int segmentId = 0;

        for(String word : words) {
            segmentText.append(word).append(" ");
            ++wordCount;
            // When we reach the target segment size or last word
            if (wordCount >= segmentSize || wordCount + segmentId * segmentSize >= totalWords) {
                String text = segmentText.toString().trim();
                double segmentDuration = (double)wordCount / wordsPerSecond;
                double endTime = currentTime + segmentDuration;
                segments.add((new Document())
                        .append("id", segmentId++)
                        .append("start", (double)Math.round(currentTime * (double)100.0F) / (double)100.0F)
                        .append("end", (double)Math.round(endTime * (double)100.0F) / (double)100.0F).append("text", text));
                currentTime = endTime;
                segmentText = new StringBuilder();
                wordCount = 0;
            }
        }

        return (new Document()).append("text", fullText).append("segments_with_timeStamps", segments);
    }

    /**
     * Original JSON parsing approach
     */
    private Document parseJsonTranscript(String transcriptData) {
        JSONObject whisperOutput = new JSONObject(transcriptData);
        // Extract the segments which contain timestamp information
        List<Document> segments = new ArrayList();

        if (whisperOutput.has("segments_with_timeStamps")) {
            JSONArray jsonSegments = whisperOutput.getJSONArray("segments_with_timeStamps");

            for(int i = 0; i < jsonSegments.length(); ++i) {
                JSONObject segment = jsonSegments.getJSONObject(i);
                Document segmentDoc = (new Document())
                        .append("id", segment.optInt("id"))
                        .append("start", segment.optDouble("start"))
                        .append("end", segment.optDouble("end"))
                        .append("text", segment.optString("text"));
                // Add word-level timestamps if available
                if (segment.has("words")) {
                    List<Document> words = new ArrayList();
                    JSONArray jsonWords = segment.getJSONArray("words");

                    for(int j = 0; j < jsonWords.length(); ++j) {
                        JSONObject word = jsonWords.getJSONObject(j);
                        words.add((new Document())
                                .append("word", word.optString("word"))
                                .append("start", word.optDouble("start"))
                                .append("end", word.optDouble("end"))
                                .append("probability", word.optDouble("probability", (double)0.0F)));
                    }

                    segmentDoc.append("words", words);
                }

                segments.add(segmentDoc);
            }
        }

        // Build the complete transcript document
        return (new Document())
                .append("text", whisperOutput.optString("text", ""))
                .append("segments_with_timeStamps", segments)
                .append("language", whisperOutput.optString("language", "unknown"));
    }

    /**
     * Parse SRT or WebVTT format (common subtitle formats)
     */
    private Document parseSRTOrVTTFormat(String rawOutput, String fullText) {
        List<Document> segments = new ArrayList();
        int segmentId = 0;
        // Split by double newline (common separator in SRT/WebVTT)
        String[] blocks = rawOutput.split("\\n\\s*\\n");

        for(String block : blocks) {
            if (!block.trim().isEmpty()) {
                String[] lines = block.trim().split("\\n");
                if (lines.length >= 2) {
                    // Find the timestamp line (contains -->)
                    String timestampLine = null;
                    StringBuilder textBuilder = new StringBuilder();

                    for(String line : lines) {
                        if (line.contains("-->")) {
                            timestampLine = line;
                        } else if (!line.matches("^\\d+$")) {
                            textBuilder.append(line).append(" ");
                        }
                    }

                    if (timestampLine != null) {
                        try {
                            // Extract start and end times
                            String[] parts = timestampLine.split("-->");
                            if (parts.length >= 2) {
                                double startTime = this.parseTimeToSeconds(parts[0].trim());
                                double endTime = this.parseTimeToSeconds(parts[1].trim());
                                String text = textBuilder.toString().trim();
                                segments.add((new Document())
                                        .append("id", segmentId++)
                                        .append("start", startTime)
                                        .append("end", endTime)
                                        .append("text", text));
                            }
                        } catch (Exception e) {
                            logger.warn("Error parsing time format: {}", timestampLine, e);
                        }
                    }
                }
            }
        }

        return (new Document()).append("text", fullText).append("segments_with_timeStamps", segments);
    }

    /**
     * Parse speaker timestamp format: [speaker_0 0.00s - 5.25s] Text
     */
    private Document parseSpeakerTimestampFormat(String rawOutput, String fullText) {
        List<Document> segments = new ArrayList();
        int segmentId = 0;
        // Split by newlines to process each line
        String[] lines = rawOutput.split("\\n");

        for(String line : lines) {
            // Pattern: [speaker_X start - end] text
            if (!line.trim().isEmpty() && line.contains("[") && line.contains("]")) {
                try {
                    int bracketOpen = line.indexOf("[");
                    int bracketClose = line.indexOf("]");
                    if (bracketOpen < bracketClose) {
                        String timestampPart = line.substring(bracketOpen + 1, bracketClose);
                        String text = line.substring(bracketClose + 1).trim();
                        // Extract start and end times
                        int dashIndex = timestampPart.lastIndexOf("-");
                        if (dashIndex > 0) {
                            String startStr = timestampPart.substring(0, dashIndex).trim();
                            String endStr = timestampPart.substring(dashIndex + 1).trim();
                            // Extract just the numeric parts with "s" suffix
                            double startTime = this.extractSeconds(startStr);
                            double endTime = this.extractSeconds(endStr);
                            segments.add((new Document()).append("id", segmentId++).append("start", startTime).append("end", endTime).append("text", text));
                        }
                    }
                } catch (Exception e) {
                    logger.warn("Error parsing speaker timestamp format: {}", line, e);
                }
            }
        }

        return (new Document()).append("text", fullText).append("segments_with_timeStamps", segments);
    }

    /**
     * Parse format with timestamps at beginning of each line
     */
    private Document parseLineByLineTimestamps(String rawOutput, String fullText) {
        List<Document> segments = new ArrayList();
        int segmentId = 0;
        // Split by newlines to process each line
        String[] lines = rawOutput.split("\\n");
        double lastEndTime = (double)0.0F;

        for(String line : lines) {
            // Try to find timestamp patterns
            if (!line.trim().isEmpty() && line.matches("^\\s*\\d+:\\d+:\\d+.*")) {
                try {
                    // Find the first space after the timestamp
                    int firstSpace = line.indexOf(32);
                    if (firstSpace > 0) {
                        String timestamp = line.substring(0, firstSpace).trim();
                        String text = line.substring(firstSpace).trim();
                        double startTime = this.parseTimeToSeconds(timestamp);
                        // If only start time is given, estimate end time
                        double endTime = startTime + (double)text.length() * 0.06; // Rough estimate based on text length

                        // Ensure continuous timing
                        if (startTime < lastEndTime) {
                            startTime = lastEndTime;
                        }

                        lastEndTime = endTime;
                        segments.add((new Document())
                                .append("id", segmentId++)
                                .append("start", startTime)
                                .append("end", endTime)
                                .append("text", text));
                    }
                } catch (Exception e) {
                    logger.warn("Error parsing line timestamp format: {}", line, e);
                }
            }
        }

        return (new Document())
                .append("text", fullText)
                .append("segments_with_timeStamps", segments);
    }

    /**
     * Helper method to parse time formats to seconds
     */
    private double parseTimeToSeconds(String timeStr) {
        timeStr = timeStr.replaceAll("[^0-9:.,]", "").trim();
        // Handle various time formats mit rs
        if (timeStr.matches("\\d+\\.\\d+")) {
            // Simple seconds format: 12.34
            return Double.parseDouble(timeStr);

        } else if (timeStr.matches("\\d+,\\d+")) {
            // Seconds with comma: 12,34
            return Double.parseDouble(timeStr.replace(',', '.'));
        } else if (timeStr.matches("\\d+:\\d+")) {
            // MM:SS format
            String[] parts = timeStr.split(":");
            int minutes = Integer.parseInt(parts[0]);
            double seconds = Double.parseDouble(parts[1]);

            return (double)(minutes * 60) + seconds;
        } else if (timeStr.matches("\\d+:\\d+:\\d+[.,]?\\d*")) {
            // HH:MM:SS format with optional milliseconds
            String[] parts = timeStr.split(":");
            int hours = Integer.parseInt(parts[0]);
            int minutes = Integer.parseInt(parts[1]);
            double seconds;
            if (parts[2].contains(",")) {
                seconds = Double.parseDouble(parts[2].replace(',', '.'));
            } else {
                seconds = Double.parseDouble(parts[2]);
            }

            return (double)(hours * 3600 + minutes * 60) + seconds;
        } else {
            throw new IllegalArgumentException("Unrecognized time format: " + timeStr);
        }
    }

    /**
     * Extract seconds value from a string that might contain non-numeric text
     */
    private double extractSeconds(String str) {
        // Remove all non-numeric characters except decimal point
        str = str.replaceAll("[^0-9.]", "");

        try {
            return Double.parseDouble(str);
        } catch (NumberFormatException var3) {
            return (double)0.0F;
        }
    }

    /**
     * Get all MP4 video files from a directory
     * @param directoryPath path to the directory containing videos
     * @return list of video file paths
     * @throws IOException if directory access fails
     */
    private static List<Path> getVideoFilesFromDirectory(Path directoryPath) throws IOException {
        if (Files.exists(directoryPath, new LinkOption[0]) && Files.isDirectory(directoryPath, new LinkOption[0])) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(directoryPath, "*.mp4")) {
                return (List)StreamSupport.stream(stream.spliterator(), false).collect(Collectors.toList());
            }
        } else {
            throw new IOException("Invalid directory path: " + String.valueOf(directoryPath));
        }
    }

    public static void main(String[] args) throws IOException {
        AppConfig config = new AppConfig();
        MongoDBHandler db = null;

        try {
            db = new MongoDBHandler();
            VideoNLPProcessor processor = new VideoNLPProcessor(db);
            // Get videos directory from config
            String videosDirectoryPath = config.getProperty("videos.directory");
            if (videosDirectoryPath != null && !videosDirectoryPath.isEmpty()) {
                Path videosDir = Paths.get(videosDirectoryPath);
                List<Path> videoFiles = getVideoFilesFromDirectory(videosDir);
                if (!videoFiles.isEmpty()) {
                    logger.info("Found {} video files to process", videoFiles.size());
                    AtomicInteger processedCount = new AtomicInteger(0);
                    int successCount = 0;

                    // Process each video file
                    for(Path videoPath : videoFiles) {
                        String filename = videoPath.getFileName().toString();

                        try {
                            logger.info("Processing video {}/{}: {}", new Object[]
                                    {processedCount.incrementAndGet(), videoFiles.size(), filename});
                            // Process with retry logic
                            int retries = 3;
                            boolean processed = false;

                            while(retries-- > 0 && !processed) {
                                try {
                                    String speechId = processor.processSpeech(videoPath);
                                    processed = true;
                                    ++successCount;
                                    logger.info("Successfully processed: {} with ID: {}",
                                            filename, speechId);
                                } catch (Exception e) {
                                    logger.error("Processing failed for {} ({} retries left): {}",
                                            new Object[]{filename, retries, e.getMessage()});
                                    if (retries > 0) {
                                        Thread.sleep(5000L);
                                        processor.reinitializePipeline();
                                    }
                                }
                            }

                            if (!processed) {
                                logger.error("Failed to process {} after multiple attempts", filename);
                            }

                            // Clean up after each video to prevent memory issues
                            System.gc();
                            Thread.sleep(1000L);
                        } catch (Exception e) {
                            logger.error("Error processing video file: {}", filename, e);
                        }
                    }

                    logger.info("Video processing complete. Successfully processed {}/{} videos",
                            successCount, videoFiles.size());
                    return;
                }

                logger.warn("No MP4 files found in directory: {}", videosDir);
                return;
            }

            logger.error("No videos directory configured in PPR_WiSe24_Project_12_4.txt");
        } catch (Exception e) {
            logger.error("Fatal error in video processing", e);
            return;
        } finally {
            if (db != null) {
                db.closeConnection();
            }

        }

    }

    public void reinitializePipeline() throws Exception {
        if (this.composer != null) {
            this.composer.shutdown();
        }

        // Reset HTTP connection manager
        HttpClientBuilder.create().build().close();
        // reinitialize the pipelines
        this.initializeComposer();
        this.initializePipeline();
    }
}

