package com.myfirstJavalin.helper;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.myfirstJavalin.config.AppConfig;
import com.myfirstJavalin.data.ElectionPeriod;
import com.myfirstJavalin.data.SpeakerDBImpl;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class XMLGenerator {
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("dd.MM.yyyy");
    private final AppConfig appConfig;
    private static final String COLLECTION_NAME = "speakers";


    /**
     * Constructor with custom AppConfig
     * @param appConfig Application configuration
     */
    public XMLGenerator(AppConfig appConfig) {
        this.appConfig = appConfig;
    }

    /**
     * Load speaker data from XML file and save to MongoDB
     * @param xmlFilePath path to the XML file
     */
    public void loadXMLAndSaveToMongoDB(String xmlFilePath) {
        try {
            // Parse XML
            List<SpeakerDBImpl> speakers = parseXML(xmlFilePath);

            // Save to MongoDB
            saveToMongoDB(speakers);

            System.out.println("Successfully imported " + speakers.size() + " speakers to MongoDB.");

        } catch (Exception e) {
            LoggerFactory.getLogger(XMLGenerator.class).error("Error processing XML or saving to MongoDB", e);
        }
    }

    /**
     * Parse XML file and extract speaker data
     * @param xmlFilePath path to the XML file
     * @return List of SpeakerDBImpl objects
     */
    private List<SpeakerDBImpl> parseXML(String xmlFilePath) throws ParserConfigurationException, SAXException, IOException, ParseException {
        List<SpeakerDBImpl> speakers = new ArrayList<>();

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        org.w3c.dom.Document document = builder.parse(new File(xmlFilePath));

        // Get all MDB elements
        NodeList mdbNodes = document.getElementsByTagName("MDB");

        for (int i = 0; i < mdbNodes.getLength(); i++) {
            Element mdbElement = (Element) mdbNodes.item(i);

            // Skip empty MDB elements
            if (!mdbElement.hasChildNodes()) {
                continue;
            }

            SpeakerDBImpl speaker = new SpeakerDBImpl();

            // Extract ID
            speaker.setSpeakerID(getElementTextContent(mdbElement, "ID"));

            // Extract name information
            Element nameElement = (Element) mdbElement.getElementsByTagName("NAME").item(0);
            if (nameElement != null) {
                speaker.setSpeakerLastName(getElementTextContent(nameElement, "NACHNAME"));
                speaker.setSpeakerFirstName(getElementTextContent(nameElement, "VORNAME"));
                speaker.setTitle(getElementTextContent(nameElement, "ANREDE_TITEL"));
                speaker.setAcademicTitle(getElementTextContent(nameElement, "AKAD_TITEL"));
            }

            // Extract biographical information
            Element bioElement = (Element) mdbElement.getElementsByTagName("BIOGRAFISCHE_ANGABEN").item(0);
            if (bioElement != null) {
                // Parse birthdate
                String birthDateStr = getElementTextContent(bioElement, "GEBURTSDATUM");
                if (!birthDateStr.isEmpty()) {
                    speaker.setDateOfBirth(DATE_FORMAT.parse(birthDateStr));
                }

                // Parse death date
                String deathDateStr = getElementTextContent(bioElement, "STERBEDATUM");
                if (!deathDateStr.isEmpty()) {
                    speaker.setDateOfDeath(DATE_FORMAT.parse(deathDateStr));
                }

                speaker.setBirthPlace(getElementTextContent(bioElement, "GEBURTSORT"));
                speaker.setGender(mapGender(getElementTextContent(bioElement, "GESCHLECHT")));
                speaker.setFamilyStatus(getElementTextContent(bioElement, "FAMILIENSTAND"));
                speaker.setReligion(getElementTextContent(bioElement, "RELIGION"));
                speaker.setJob(getElementTextContent(bioElement, "BERUF"));
                speaker.setParty(getElementTextContent(bioElement, "PARTEI_KURZ"));
                String vitaKurz = getElementTextContent(bioElement, "VITAKURZ");
                speaker.setVita(getElementTextContent(bioElement,"VITA_KURZ"));

            }

            // Extract election periods
            NodeList periodNodes = mdbElement.getElementsByTagName("WAHLPERIODE");
            List<ElectionPeriod> electionPeriods = new ArrayList<>();

            for (int j = 0; j < periodNodes.getLength(); j++) {
                Element periodElement = (Element) periodNodes.item(j);
                ElectionPeriod period = new ElectionPeriod();

                period.setPeriodNumber(getElementTextContent(periodElement, "WP"));
                System.out.println("Period Number: " + period.getPeriodNumber());

                // Parse start date
                String startDateStr = getElementTextContent(periodElement, "MDBWP_VON");
                if (!startDateStr.isEmpty()) {
                    period.setStartDate(DATE_FORMAT.parse(startDateStr));
                    System.out.println("Start Date: " + startDateStr);
                }

                // Parse end date
                String endDateStr = getElementTextContent(periodElement, "MDBWP_BIS");
                if (!endDateStr.isEmpty()) {
                    period.setEndDate(DATE_FORMAT.parse(endDateStr));
                    System.out.println("End Date: " + endDateStr);
                }

                electionPeriods.add(period);

                // Extract fraction information (from the first institution in the first period)
                if (j == 0) {
                    NodeList institutionNodes = periodElement.getElementsByTagName("INSTITUTION");
                    if (institutionNodes.getLength() > 0) {
                        Element institutionElement = (Element) institutionNodes.item(0);
                        speaker.setFraction(getElementTextContent(institutionElement, "INS_LANG"));
                    }
                }
            }

            speaker.setElectionPeriods(electionPeriods);
            speakers.add(speaker);
        }

        return speakers;
    }

    /**
     * Save speakers to MongoDB
     * @param speakers List of SpeakerDBImpl objects
     */
    private void saveToMongoDB(List<SpeakerDBImpl> speakers) {
        if (appConfig == null) {
            System.err.println("AppConfig is not initialized. Cannot connect to MongoDB.");
            return;
        }

        try (MongoClient mongoClient = MongoClients.create(appConfig.getMongoUri())) {
            MongoDatabase database = mongoClient.getDatabase(appConfig.getMongoDatabase());
            MongoCollection<org.bson.Document> collection = database.getCollection(COLLECTION_NAME);

            // Create collection if it doesn't exist
            boolean collectionExists = false;
            for (String collName : database.listCollectionNames()) {
                if (collName.equals(COLLECTION_NAME)) {
                    collectionExists = true;
                    break;
                }
            }

            if (!collectionExists) {
                database.createCollection(COLLECTION_NAME);
                System.out.println("Created collection: " + COLLECTION_NAME);
            }

            // Clear existing data
            collection.drop();
            collection = database.getCollection(COLLECTION_NAME);

            // Insert speakers
            for (SpeakerDBImpl speaker : speakers) {
                org.bson.Document speakerDoc = convertSpeakerToDocument(speaker);
                collection.insertOne(speakerDoc);
            }

            System.out.println("Successfully saved " + speakers.size() + " speakers to MongoDB.");
        } catch (Exception e) {
            System.err.println("Error connecting to MongoDB: " + e.getMessage());
            LoggerFactory.getLogger(XMLGenerator.class).error("Error connecting to MongoDB", e);
        }
    }

    /**
     * Convert SpeakerDBImpl to MongoDB Document
     * @param speaker SpeakerDBImpl object
     * @return MongoDB Document
     */
    private org.bson.Document convertSpeakerToDocument(SpeakerDBImpl speaker) {
        org.bson.Document speakerDoc = new org.bson.Document();

        speakerDoc.append("speakerID", speaker.getSpeakerID())
                .append("speakerLastName", speaker.getSpeakerLastName())
                .append("speakerFirstName", speaker.getSpeakerFirstName())
                .append("title", speaker.getTitle())
                .append("academicTitle", speaker.getAcademicTitle())
                .append("gender", speaker.getGender())
                .append("birthPlace", speaker.getBirthPlace())
                .append("familyStatus", speaker.getFamilyStatus())
                .append("religion", speaker.getReligion())
                .append("job", speaker.getJob())
                .append("vita", speaker.getVita())
                .append("party", speaker.getParty())
                .append("fraction", speaker.getFraction());

        // Add dates
        if (speaker.getDateOfBirth() != null) {
            speakerDoc.append("dateOfBirth", speaker.getDateOfBirth());
        }

        if (speaker.getDateOfDeath() != null) {
            speakerDoc.append("dateOfDeath", speaker.getDateOfDeath());
        }

        // Add election periods
        List<org.bson.Document> periodsDoc = new ArrayList<>();
        List<ElectionPeriod> periods = speaker.getElectionPeriods();
        if (periods != null) {
            for (ElectionPeriod period : periods) {
                org.bson.Document periodDoc = new org.bson.Document()
                        .append("periodNumber", period.getPeriodNumber());

                if (period.getStartDate() != null) {
                    periodDoc.append("startDate", period.getStartDate());
                }

                if (period.getEndDate() != null) {
                    periodDoc.append("endDate", period.getEndDate());
                }

                periodsDoc.add(periodDoc);
            }
        }

        if (!periodsDoc.isEmpty()) {
            speakerDoc.append("electionPeriods", periodsDoc);
        }
        return speakerDoc;
    }

    /**
     * Get text content of an element
     * @param parent Parent element
     * @param elementName Name of the child element
     * @return Text content of the element or empty string if not found
     */
    private String getElementTextContent(Element parent, String elementName) {
        NodeList nodeList = parent.getElementsByTagName(elementName);
        if (nodeList.getLength() > 0) {
            return nodeList.item(0).getTextContent().trim();
        }
        return "";
    }

    /**
     * Map gender from German to English
     * @param genderDE German gender description
     * @return English gender description
     */
    private String mapGender(String genderDE) {
        if (genderDE.equals("männlich")) {
            return "male";
        } else if (genderDE.equals("weiblich")) {
            return "female";
        } else {
            return genderDE;
        }
    }

    /**
     * Main method to run the import process
     * @param args Command line arguments
     */
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.out.println("Usage: java XMLGenerator <xml-file-path>");
            return;
        }

        String xmlFilePath = args[0];
        AppConfig appConfig = new AppConfig();
        XMLGenerator generator = new XMLGenerator(appConfig);
        generator.loadXMLAndSaveToMongoDB(xmlFilePath);
    }
}