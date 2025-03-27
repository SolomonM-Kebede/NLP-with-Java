package com.myfirstJavalin.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import freemarker.template.Configuration;
import io.javalin.Javalin;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Handles the rendering of templates in the application using FreeMarker.
 * Manages data fetching and prepares the context for rendering templates.
 */
public class TemplateHandler {

    private static final Logger logger = LoggerFactory.getLogger(TemplateHandler.class);
    private final RESTHandler restHandler;
    private final Configuration freemarkerConfig;

    /**
     * Constructs a new TemplateHandler.
     *
     * @param restHandler the RESTHandler instance for interacting with the application.
     * @param sBasePath   the base path for the FreeMarker template directory.
     * @throws IOException if the template directory cannot be loaded.
     */
    public TemplateHandler(RESTHandler restHandler, String sBasePath) throws IOException {
        this.freemarkerConfig = new Configuration(Configuration.VERSION_2_3_23);
        freemarkerConfig.setDefaultEncoding("UTF-8");
        freemarkerConfig.setDirectoryForTemplateLoading(new File(sBasePath));
        this.restHandler = restHandler;

        // Set up template routes
        setupTemplateRoutes();
    }

    /**
     * Sets up all template-related routes
     */
    private void setupTemplateRoutes() {
        Javalin app = restHandler.getApp();

        // Home route
        app.get("/home", ctx -> {
            List<String> speechIds = restHandler.getDBConnection().getMongoDatabase()
                    .getCollection("nlpSpeech")
                    .distinct("_id", String.class)
                    .into(new ArrayList<>());

            Map<String, Object> model = new HashMap<>();
            model.put("appName", "Speech Analysis Portal");
            model.put("speechIds", speechIds);
            ctx.render("templates/home.ftl", model);
        });
    }
}