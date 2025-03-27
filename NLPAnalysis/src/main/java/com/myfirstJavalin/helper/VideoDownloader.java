package com.myfirstJavalin.helper;

import org.apache.commons.io.FileUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class VideoDownloader {

    public static void main(String[] args) {
        // Define the base URL structure
        String baseUrl = "https://cldf-od.r53.cdn.tv1.eu/1000153copo/ondemand/app144277506/145293313/";

        // Define the speech and video ID ranges
        int startSpeechId = 2019614200; // the first speech ID
        int startVideoId = 7617697; // First video ID
        int endVideoId = 7617707;  // Last video ID

        // Loop through the range of video IDs
        for (int videoId = startVideoId, speechId = startSpeechId; videoId <= endVideoId; videoId++, speechId += 100) {
            String videoUrl = baseUrl + videoId + "/" + videoId + "_h264_1920_1080_5000kb_baseline_de_5000.mp4?fdl=1";

            // Generate the filename
            String fileName = "ID" + speechId + "_TOP 5_" + videoId + "_h264_1920_1080_5000kb_baseline_de_5000.mp4";

            // Call method to download video
            System.out.println("Downloading: " + videoUrl);
            downloadVideo(videoUrl, fileName);
        }
    }

    public static void downloadVideo(String videoUrl, String outputFile) {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpGet request = new HttpGet(videoUrl);

            try (CloseableHttpResponse response = client.execute(request)) {
                HttpEntity entity = response.getEntity();

                if (response.getStatusLine().getStatusCode() == 200) {
                    FileUtils.copyInputStreamToFile(entity.getContent(), new File(outputFile));
                    System.out.println("Video downloaded successfully: " + outputFile);
                } else {
                    System.out.println("Failed to download video. HTTP Code: " + response.getStatusLine().getStatusCode());
                }
            }
        } catch (IOException e) {
            LoggerFactory.getLogger(VideoDownloader.class).error("Error downloading: {}", videoUrl, e);
        }
    }
}