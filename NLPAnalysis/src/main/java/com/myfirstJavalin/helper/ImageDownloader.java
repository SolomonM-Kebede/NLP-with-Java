package com.myfirstJavalin.helper;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.myfirstJavalin.config.AppConfig;
import org.apache.commons.io.IOUtils;
import org.bson.Document;
import org.bson.types.Binary;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ImageDownloader {

    private static final Logger LOGGER = Logger.getLogger(ImageDownloader.class.getName());
    private final MongoCollection<Document> deputiesCollection;

    public ImageDownloader(MongoCollection<Document> deputiesCollection) {
        this.deputiesCollection = deputiesCollection;
    }

    public void downloadSpeakerImages() {
        deputiesCollection.find().forEach(this::processSpeakerImage);
    }

    private void processSpeakerImage(Document deputyDoc) {

        try {
            String speakerId = getStringValue(deputyDoc, "speakerID");
            String fullName = extractFullName(deputyDoc);

            System.out.println("Processing speaker: " + fullName + " (ID: " + speakerId + ")");

            if (fullName == null || fullName.trim().isEmpty()) {
                System.err.println("CRITICAL: No valid name for speaker ID " + speakerId);
                return;
            }

            // Check if the document already has an image
            if (deputyDoc.containsKey("image") && deputyDoc.get("image") instanceof Document imageDoc) {
                if (imageDoc.containsKey("data") && imageDoc.get("data") instanceof Binary) {
                    System.out.println("Image already exists for " + fullName + ", skipping download.");
                    return;
                }
            }

            ImageMetadata imageMetadata = findDeputyImageMetadata(fullName);

            if (imageMetadata == null) {
                System.err.println("NO IMAGE METADATA FOUND for: " + fullName);
                return;
            }

            System.out.println("Image Metadata Found: " +
                    "URL=" + imageMetadata.imageUrl +
                    ", Source=" + imageMetadata.sourceUrl
            );

            byte[] imageData = downloadImageAsByteArray(imageMetadata.imageUrl);

            if (imageData == null || imageData.length == 0) {
                System.err.println("FAILED to download image for: " + fullName);
                return;
            }

            System.out.println("Image downloaded successfully for: " + fullName +
                    " (Size: " + imageData.length + " bytes)");

            // Update deputy document with image binary data and metadata
            Document imageDocument = new Document()
                    .append("data", new Binary(imageData))
                    .append("contentType", "image/jpeg")
                    .append("source", imageMetadata.sourceUrl)
                    .append("title", imageMetadata.title)
                    .append("metadata", imageMetadata.additionalMetadata);

            Document updateDoc = new Document("$set", new Document("image", imageDocument));

            deputiesCollection.updateOne(
                    new Document("speakerID", speakerId),
                    updateDoc
            );

            System.out.println("Successfully updated MongoDB with image data for: " + fullName);

        } catch (Exception e) {
            LOGGER.info("CRITICAL ERROR processing speaker image:"+ e);
        }
    }

    // Robust method to get string value from document
    private String getStringValue(Document doc, String key) {
        Object value = doc.get(key);
        if (value == null) return null;

        if (value instanceof String) {
            return (String) value;
        }

        return value.toString();
    }

    private String extractFullName(Document deputyDoc) {
        String firstName = getStringValue(deputyDoc, "speakerFirstName");
        String lastName = getStringValue(deputyDoc, "speakerLastName");

        if (firstName != null && lastName != null) {
            return firstName + " " + lastName;
        }

        // Fallback to handling combined name if separate fields don't exist
        Object nameObj = deputyDoc.get("fullName");
        if (nameObj != null) {
            return nameObj.toString();
        }

        return null;
    }

    private ImageMetadata findDeputyImageMetadata(String fullName) {
        try {
            // URL encode the full name with additional handling
            String encodedName = URLEncoder.encode(fullName, StandardCharsets.UTF_8)
                    .replace("+", "%20");

            String IMAGE_BASE_URL = "https://bilddatenbank.bundestag.de/search/picture-result?query=";
            String searchUrl = IMAGE_BASE_URL + encodedName;

            LOGGER.info("Searching for image with full name: " + fullName);
            LOGGER.info("Encoded search URL: " + searchUrl);

            org.jsoup.nodes.Document doc = Jsoup.connect(searchUrl)
                    .userAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
                    .referrer("https://bilddatenbank.bundestag.de/")
                    .timeout(30000) // Increased timeout
                    .maxBodySize(0) // No limit on body size
                    .ignoreHttpErrors(true)
                    .get();

            // Look for links with data-fancybox="group" attribute
            Elements imageLinks = doc.select("a[data-fancybox='group']");

            if (imageLinks.isEmpty()) {
                LOGGER.warning("No images found using primary selector. Trying fallback selectors for: " + fullName);

                // Try fallback selectors
                imageLinks = doc.select("a[href*='/picture-detail']");

                if (imageLinks.isEmpty()) {
                    LOGGER.warning("NO IMAGE LINKS FOUND for: " + fullName);
                    return null;
                }
            }

            // Find the most relevant image by checking if the alt text contains the person's name
            Element bestMatch = null;
            for (Element link : imageLinks) {
                Element img = link.selectFirst("img");
                if (img != null) {
                    String altText = img.attr("alt").toLowerCase();
                    // Check if the alt text contains the person's last name
                    String[] nameParts = fullName.split(" ");
                    String lastName = nameParts[nameParts.length - 1].toLowerCase();

                    if (altText.contains(lastName)) {
                        bestMatch = link;
                        break;
                    }
                }
            }

            // If no match found by name, just use the first image
            if (bestMatch == null && !imageLinks.isEmpty()) {
                bestMatch = imageLinks.first();
                LOGGER.info("No exact name match found. Using first available image for: " + fullName);
            }

            if (bestMatch == null) {
                LOGGER.warning("NO SUITABLE IMAGE FOUND for: " + fullName);
                return null;
            }

            // Get the detail page URL
            String detailPageUrl = bestMatch.attr("href");
            if (!detailPageUrl.startsWith("http")) {
                detailPageUrl = "https://bilddatenbank.bundestag.de" + detailPageUrl;
            }
            LOGGER.info("Found detail page URL: " + detailPageUrl);

            // Get the image URL directly from the srcset attribute or other sources
            Element imgElement = bestMatch.selectFirst("img");
            String imageUrl = "";

            if (imgElement != null) {
                // Try to get the high-resolution image URL from the parent's data-srcset
                String srcset = bestMatch.attr("data-srcset");
                if (!srcset.isEmpty()) {
                    // Extract the first URL from srcset (highest resolution)
                    String[] srcsetParts = srcset.split(" ");
                    if (srcsetParts.length > 0) {
                        imageUrl = srcsetParts[0];
                    }
                }

                // Fallback to the src attribute if srcset parsing fails
                if (imageUrl.isEmpty()) {
                    imageUrl = imgElement.attr("src");
                }

                // Ensure the URL is absolute
                if (!imageUrl.startsWith("http")) {
                    imageUrl = "https://bilddatenbank.bundestag.de" + imageUrl;
                }

                LOGGER.info("Image URL found: " + imageUrl);

                // Extract additional metadata from data-caption
                Map<String, String> metadata = new HashMap<>();
                String caption = bestMatch.attr("data-caption");
                String title = bestMatch.attr("aria-label");

                // Add the caption as metadata
                if (!caption.isEmpty()) {
                    metadata.put("caption", caption);
                }

                // Add the title as metadata
                if (!title.isEmpty()) {
                    metadata.put("description", title);
                }

                return new ImageMetadata(
                        imageUrl,
                        detailPageUrl,
                        imgElement.attr("alt"),
                        metadata
                );
            }

            LOGGER.warning("No image element found within the link for: " + fullName);
            return null;

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Comprehensive error in image search for " + fullName, e);
            return null;
        }
    }

    private byte[] downloadImageAsByteArray(String imageUrl) {
        int maxRetries = 3;
        Exception lastException = null;

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                URLConnection connection = getUrlConnection(imageUrl);

                // Download the image directly as byte array
                try (InputStream inputStream = connection.getInputStream()) {
                    byte[] imageData = IOUtils.toByteArray(inputStream);
                    if (imageData.length > 0) {
                        return imageData;
                    } else {
                        System.err.println("Downloaded image has zero bytes");
                    }
                }
            } catch (IOException e) {
                lastException = e;
                System.err.println("Attempt " + attempt + " failed: " + e.getMessage());

                if (attempt < maxRetries) {
                    try {
                        // Wait before retrying with exponential backoff
                        Thread.sleep(2000L * attempt);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        System.err.println("Sleep interrupted: " + ie.getMessage());
                    }
                }
            }
        }

        if (lastException != null) {
            System.err.println("All download attempts failed:");
            LOGGER.log(Level.SEVERE, lastException.getMessage(), lastException);
        }

        return null;
    }

    private static URLConnection getUrlConnection(String imageUrl) throws IOException {
        URL url = new URL(imageUrl);

        // Configure connection
        URLConnection connection = url.openConnection();
        connection.setConnectTimeout(30000);
        connection.setReadTimeout(60000);
        // Set user agent to mimic browser
        connection.setRequestProperty("User-Agent",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36");
        connection.setRequestProperty("Referer", "https://bilddatenbank.bundestag.de/");
        return connection;
    }

    // Inner class to hold image metadata
    private static class ImageMetadata {
        String imageUrl;
        String sourceUrl;
        String title;
        Map<String, String> additionalMetadata;

        ImageMetadata(String imageUrl, String sourceUrl, String title, Map<String, String> metadata) {
            this.imageUrl = imageUrl;
            this.sourceUrl = sourceUrl;
            this.title = title;
            this.additionalMetadata = metadata;
        }
    }

    // Main method for running and connecting
    public static void main(String[] args) throws IOException {
        AppConfig appConfig = new AppConfig();
        MongoClient mongoClient = MongoClients.create(appConfig.getProperty("mongo.uri"));
        MongoDatabase database = mongoClient.getDatabase(appConfig.getProperty("mongo.database"));
        MongoCollection<Document> deputiesCollection = database.getCollection("speakers");

        ImageDownloader downloader = new ImageDownloader(deputiesCollection);
        downloader.downloadSpeakerImages();
    }
}
