package com.myfirstJavalin.helper;


import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;

public class WebScraper {
    private static final String AJAX_URL_TEMPLATE = "https://www.bundestag.de/ajax/filterlist/de/services/opendata/866354-866354?limit=10&noFilterSet=true&offset=";

    public static void main(String[] args) {
        try {
            int offset = 0; // Start at first page (203–212)
            while (true) {
                // Construct URL with offset
                String ajaxUrl = AJAX_URL_TEMPLATE + offset;
                System.out.println("Fetching: " + ajaxUrl);

                // Fetch the AJAX response page
                Document doc = Jsoup.connect(ajaxUrl).get();

                // Extract XML links
                Elements links = doc.select("a.bt-link-dokument[href$=.xml]");

                if (links.isEmpty()) {
                    System.out.println("No more XML links found. Stopping.");
                    break;
                }

                for (Element link : links) {
                    String xmlUrl = link.attr("abs:href");
                    System.out.println("Found XML: " + xmlUrl);
                    downloadXml(xmlUrl);
                }

                // Increase offset to get next batch (next 10 results)
                offset += 10;
            }

        } catch (IOException e) {
            LoggerFactory.getLogger(WebScraper.class).error(e.getMessage(), e);
        }
    }

    private static void downloadXml(String xmlUrl) {
        try (InputStream in = new URL(xmlUrl).openStream()) {
            String fileName = "downloaded_" + xmlUrl.substring(xmlUrl.lastIndexOf("/") + 1);
            Files.copy(in, Paths.get(fileName));
            System.out.println("Downloaded: " + fileName);
        } catch (IOException e) {
            System.err.println("Error downloading: " + xmlUrl);
            LoggerFactory.getLogger(WebScraper.class).error(e.getMessage(), e);
        }
    }
}