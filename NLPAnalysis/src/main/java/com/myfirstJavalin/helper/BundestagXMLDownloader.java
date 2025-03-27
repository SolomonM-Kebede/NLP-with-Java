package com.myfirstJavalin.helper;

import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.io.File;

public class BundestagXMLDownloader {
    private static final String USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36";
    private static final String BASE_URL = "https://www.bundestag.de/resource/blob/";
    private static final String DOWNLOAD_DIRECTORY = "./downloaded_plenarprotokolle/";
    private static final int MAX_RETRIES = 3;
    private static final int RETRY_DELAY_MS = 1000;
    private static final int WAHLPERIODE = 20;

    public static void main(String[] args) {
        try {
            // Create download directory if it doesn't exist
            File directory = new File(DOWNLOAD_DIRECTORY);
            if (!directory.exists()) {
                directory.mkdirs();
            }

            // Generate URLs for all potential sitzungen in Wahlperiode 20
            // We'll try Sitzungen 1 through 250 to be safe
            List<String> potentialUrls = generatePotentialUrls();
            System.out.println("Generated " + potentialUrls.size() + " potential URLs to try.");

            // Try to download each potential XML file
            int successCount = 0;
            for (int i = 0; i < potentialUrls.size(); i++) {
                String xmlUrl = potentialUrls.get(i);
                String fileName = "wp" + WAHLPERIODE + "_sitzung" + (i+1) + ".xml";
                String destinationPath = DOWNLOAD_DIRECTORY + fileName;

                System.out.println("Trying URL " + (i+1) + "/" + potentialUrls.size() + ": " + xmlUrl);

                if (downloadXml(xmlUrl, destinationPath)) {
                    successCount++;
                    System.out.println("Successfully downloaded: " + fileName);
                } else {
                    // Remove the file if download was unsuccessful
                    new File(destinationPath).delete();
                }

                // Add a small delay to avoid overwhelming the server
                Thread.sleep(500);
            }

            System.out.println("Download complete. Successfully downloaded " + successCount + " files.");
        } catch (Exception e) {
            LoggerFactory.getLogger(BundestagXMLDownloader.class).error(e.getMessage(), e);
        }
    }

    /**
     * Generate a list of potential URLs for Wahlperiode and Sitzung numbers
     */
    private static List<String> generatePotentialUrls() {
        List<String> urls = new ArrayList<>();


        // We'll try a few different URL patterns since we don't know the exact format
        for (int sitzung = 1; sitzung <= 250; sitzung++) {
            // Pattern 1: Direct enumeration (most common)
            String sitzungStr = String.format("%d%03d", BundestagXMLDownloader.WAHLPERIODE, sitzung);
            urls.add(BASE_URL + sitzungStr + ".xml");

        }

        return urls;
    }


    /**
     * Download an XML file from the given URL to the specified destination path
     */
    private static boolean downloadXml(String urlStr, String destinationPath) {
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                // Create a URL object from the provided URL string
                HttpURLConnection connection = getHttpURLConnection(urlStr);

                // Check the response code (200 means OK)
                int responseCode = connection.getResponseCode();
                if (responseCode == HttpURLConnection.HTTP_OK) {
                    // Check content type to ensure it's XML
                    String contentType = connection.getContentType();
                    if (contentType != null && !contentType.toLowerCase().contains("xml")) {
                        System.out.println("Skipping non-XML content: " + contentType);
                        return false;
                    }

                    // Get the input stream to read the content
                    try (InputStream inputStream = connection.getInputStream()) {
                        // Create a file output stream to save the downloaded file
                        try (FileOutputStream fileOutputStream = new FileOutputStream(destinationPath)) {
                            byte[] buffer = new byte[4096];
                            int bytesRead;
                            long totalBytes = 0;

                            while ((bytesRead = inputStream.read(buffer)) != -1) {
                                fileOutputStream.write(buffer, 0, bytesRead);
                                totalBytes += bytesRead;
                            }

                            // If file size is too small, it might not be a valid XML
                            if (totalBytes < 1000) {
                                System.out.println("Skipping small file (probably not valid XML): " + totalBytes + " bytes");
                                return false;
                            }

                            return true;
                        }
                    }
                } else if (responseCode == HttpURLConnection.HTTP_NOT_FOUND) {
                    // 404 means the file doesn't exist, no need to retry
                    return false;
                } else {
                    System.out.println("Attempt " + attempt + " failed. HTTP Response Code: " + responseCode);
                    if (attempt < MAX_RETRIES) {
                        System.out.println("Retrying in " + (RETRY_DELAY_MS / 1000) + " seconds...");
                        Thread.sleep(RETRY_DELAY_MS);
                    }
                }
            } catch (IOException | InterruptedException e) {
                System.err.println("Attempt " + attempt + " failed: " + e.getMessage());
                if (attempt < MAX_RETRIES) {
                    try {
                        System.out.println("Retrying in " + (RETRY_DELAY_MS / 1000) + " seconds...");
                        Thread.sleep(RETRY_DELAY_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
        return false;
    }

    @NotNull
    private static HttpURLConnection getHttpURLConnection(String urlStr) throws IOException {
        URL url = new URL(urlStr);

        // Open a connection to the URL
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();

        // Set the request method (GET)
        connection.setRequestMethod("GET");

        // Set the User-Agent header to simulate a browser request
        connection.setRequestProperty("User-Agent", USER_AGENT);
        connection.setRequestProperty("Accept", "text/html,application/xhtml+xml,application/xml");
        connection.setRequestProperty("Accept-Language", "en-US,en;q=0.9,de;q=0.8");
        connection.setRequestProperty("Referer", "https://www.bundestag.de/");
        connection.setRequestProperty("Connection", "keep-alive");

        // Connect to the URL
        connection.connect();
        return connection;
    }

}