package com.myfirstJavalin.controller;

import com.myfirstJavalin.database.MongoDBHandler;
import io.javalin.Javalin;
import io.javalin.http.HttpStatus;
import io.javalin.http.staticfiles.Location;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.*;

/**
 * Handles REST API endpoints and connects with the database layer.
 * Also responsible for starting the Javalin server.
 */
public class RESTHandler {
    private static final Logger logger = LoggerFactory.getLogger(RESTHandler.class);
    private final Javalin app;
    private final MongoDBHandler dbConnection;
    private static final int DEFAULT_PORT = 1821;

    /**
     * Constructor to initialize the RESTHandler with a new Javalin instance.
     *
     * @param dbConnection The DBConnection instance for database interaction.
     * @throws IOException if there is an error during the setup of routes or template handler.
     */
    public RESTHandler(MongoDBHandler dbConnection) throws IOException {
        this(dbConnection, DEFAULT_PORT);
    }

    /**
     * Constructor to initialize the RESTHandler with a new Javalin instance on a specific port.
     *
     * @param dbConnection The DBConnection instance for database interaction.
     * @param port The port on which to start the Javalin server.
     * @throws IOException if there is an error during the setup of routes or template handler.
     */
    public RESTHandler(MongoDBHandler dbConnection, int port) throws IOException {
        if (dbConnection == null) {
            throw new IllegalArgumentException("DBConnection cannot be null.");
        }

        this.dbConnection = dbConnection;

        // Create and configure Javalin
        this.app = Javalin.create(config -> {
            config.staticFiles.add(staticFileConfig -> {
                staticFileConfig.directory = "static";
                staticFileConfig.hostedPath = "/static";
                staticFileConfig.location = Location.CLASSPATH;
            });
        }).start(port);  // Start server

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down Javalin server...");
            this.app.stop();
        }));

        logger.info("Javalin server started on port {}", port);

        // Initialize the API routes
        this.initAPIRoutes();

        // Initialize TemplateHandler after API routes are set up
        TemplateHandler portfolioTemplateHandler = new TemplateHandler(this, "./src/main/resources/templates");

        logger.info("REST API endpoints initialized successfully");
    }

    /**
     * Returns the DBConnection instance.
     * @return The DBConnection instance.
     */
    public MongoDBHandler getDBConnection() {
        if (this.dbConnection == null) {
            throw new NullPointerException("DBConnection is null");
        }
        return this.dbConnection;
    }

    /**
     * Returns the Javalin app instance.
     * @return The Javalin app instance.
     */
    public Javalin getApp() {
        return this.app;
    }

    /**
     * Initializes all routes for the REST API.
     */
    private void initAPIRoutes() {
        // Root status route
        this.app.get("/", ctx -> {
            JSONObject response = new JSONObject();
            response.put("Status: ", "It is Working!");
            ctx.json(response.toString());
            ctx.status(HttpStatus.ACCEPTED);
        });

        // Get all speech IDs
        this.app.get("/api/speeches", ctx -> {
            List<String> speechIds = dbConnection.getAllSpeechIds();
            ctx.json(speechIds);
        });


    }
}