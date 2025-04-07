package com.myfirstJavalin.speech;

import org.bson.Document;
import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

public class SpeechHandler extends DefaultHandler {
    private final List<Document> speeches = new ArrayList<>();
    private final List<Document> agendaItems = new ArrayList<>();
    private final List<Document> comments = new ArrayList<>();

    private Document currentSpeech = null;
    private Document currentAgenda = null; // Stores the current agenda
    private Document currentComment = null; // Store the current comment
    private Document currentProtocol = null; // Stores the protocol information
    private final StringBuilder currentText = new StringBuilder();
    private String currentSpeakerID = null;
    private String currentSpeakerFirstName = null;
    private String currentSpeakerLastName = null;
    private String currentSpeakerParty = null; // Party of the speaker
    private String fullTextSpeech = null;
    private final List<Document> currentTextContentObjects = new ArrayList<>();
    private String currentDate = null;
    private boolean inRede = false;   // Inside <rede> element
    private boolean inAgendaTitle = false; // Inside <ivz-block-titel>
    private boolean inAgendaContent = false; // Inside <ivz-eintrag-inhalt>
    private boolean inKommentar = false; // Inside <kommentar> element

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) {
        currentText.setLength(0); // Reset text buffer for each element

        if ("rede".equals(qName)) {
            // Initialize a new speech document
            currentSpeech = new Document();
            String speechId = attributes.getValue("id");
            currentSpeech.append("_id", speechId);
            inRede = true;

            // Reset speaker information and text content for this speech
            currentSpeakerID = null;
            currentSpeakerFirstName = null;
            currentSpeakerLastName = null;
            currentSpeakerParty = null;
            currentTextContentObjects.clear();
        } else if ("redner".equals(qName)) {
            // Element tracking
            // Inside <redner> element
            currentSpeakerID = attributes.getValue("id");
        } else if ("tagesordnungspunkt".equals(qName)) {
            // Create a new agenda item
            currentAgenda = new Document();
            String agendaId = attributes.getValue("id");
            String topId = attributes.getValue("top-id");
            String titel = attributes.getValue("titel");
            String inhalt = attributes.getValue("inhalt");

            // Use a unique ID if none provided
            if (agendaId == null) {
                agendaId = "agenda-" + Math.abs(UUID.randomUUID().hashCode());
            }

            currentAgenda.append("_id", agendaId);
            if (topId != null) currentAgenda.append("index", topId);
            if (titel != null) currentAgenda.append("title", titel);
            if (inhalt != null) currentAgenda.append("content", inhalt);
        } else if ("ivz-block-titel".equals(qName)) {
            inAgendaTitle = true;
        } else if ("ivz-eintrag-inhalt".equals(qName)) {
            inAgendaContent = true;
        } else if ("kopfdaten".equals(qName)) {
            // Initialize protocol document
            currentProtocol = new Document();
        } else if ("datum".equals(qName)) {
            // Extract date attribute from <datum>
            currentDate = attributes.getValue("date");
        } else if ("kommentar".equals(qName)) {
            inKommentar = true;
            currentComment = new Document();
            String speechId = currentSpeech != null && currentSpeech.containsKey("_id") ?
                    currentSpeech.getString("_id") : "unknown";

            String commentId = speechId+ "-" + Math.abs(UUID.randomUUID().hashCode());
            currentComment.put("_id", commentId);

            // Set the speech ID reference
            if (currentSpeech != null && currentSpeech.containsKey("_id")) {
                currentComment.put("speechId", currentSpeech.getString("_id"));
            } else {
                currentComment.put("speechId", "unknown");
            }

            // Set the speaker ID reference
            if (currentSpeakerID != null) {
                currentComment.put("speakerId", currentSpeakerID);
            } else {
                currentComment.put("speakerId", "unknown");
            }

            // Set the timestamp
            currentComment.put("date", System.currentTimeMillis());

            System.out.println("Started processing comment with ID: " + commentId);
        }
    }

    @Override
    public void characters(char[] ch, int start, int length) {
        // Append text content for the current element
        currentText.append(ch, start, length);
    }

    @Override
    public void endElement(String uri, String localName, String qName) {
        String text = currentText.toString().trim();

        if ("p".equals(qName)) {
            addTextContent("text", text);
        } else if ("kommentar".equals(qName)) {
            inKommentar = false;

            // Add the comment text to the current comment document
            if (currentComment != null && !text.isEmpty()) {
                currentComment.put("text", text);

                // Add the comment to the comments list
                comments.add(currentComment);
                System.out.println("Added comment: " + currentComment.getString("_id"));

                // Add text content for speech
                addTextContent("comment", text);

                // Reset current comment
                currentComment = null;
            }
        } else if ("vorname".equals(qName)) {
            currentSpeakerFirstName = text;
        } else if ("nachname".equals(qName)) {
            currentSpeakerLastName = text;
        } else if ("fraktion".equals(qName)) {
            currentSpeakerParty = text.isEmpty() ? null : text;
        } else if ("ivz-block-titel".equals(qName)) {
            inAgendaTitle = false;
            if (currentAgenda != null) {
                currentAgenda.append("index", text);
                currentAgenda.append("title", text);
            }
        } else if ("ivz-eintrag-inhalt".equals(qName)) {
            inAgendaContent = false;
            if (currentAgenda != null) {
                String existingContent = currentAgenda.getString("content");
                if (existingContent == null) existingContent = "";
                existingContent += (existingContent.isEmpty() ? "" : "; ") + text;
                currentAgenda.append("content", existingContent);

                // Clone the current agenda item and add it to the global list
                Document agendaToSave = new Document(currentAgenda);
                // Ensure we have a unique ID for each agenda item
                if (!agendaToSave.containsKey("_id")) {
                    agendaToSave.put("_id", "agenda-" + UUID.randomUUID().toString());
                }
                agendaItems.add(agendaToSave);
            }
        } else if ("rede".equals(qName)) {
            finalizeCurrentSpeech();
        } else if ("wahlperiode".equals(qName)) {
            // Sanitize input to remove non-numeric characters
            String sanitizedText = text.replaceAll("[^0-9]", ""); // Keep only digits
            if (!sanitizedText.isEmpty()) {
                currentProtocol.append("wp", Integer.parseInt(sanitizedText));
            } else {
                currentProtocol.append("wp", null); // Default to null if no valid number exists
            }
        } else if ("sitzungsnr".equals(qName)) {
            // Sanitize input to remove non-numeric characters
            String sanitizedText = text.replaceAll("[^0-9]", "");
            if (!sanitizedText.isEmpty()) {
                currentProtocol.append("index", Integer.parseInt(sanitizedText));
            } else {
                currentProtocol.append("index", null); // Default to null if no valid number exists
            }
        } else if ("ort".equals(qName)) {
            currentProtocol.append("place", text); // Add location (Ort)
        } else if ("datum".equals(qName)) {
            long timestamp = parseDate(currentDate);
            currentProtocol.append("date", timestamp)
                    .append("starttime", timestamp)
                    .append("endtime", timestamp);
        } else if ("sitzungstitel".equals(qName)) {
            currentProtocol.append("title", text); // Add session title (Sitzungstitel)
        } else if ("kopfdaten".equals(qName)) {
            finalizeProtocol(); // Finalize protocol information
        }
    }

    private void addTextContent(String type, String text) {
        if (currentSpeech != null && !text.isEmpty()) {
            String speechId = currentSpeech.getString("_id");
            Document textDoc = new Document()
                    .append("id", speechId + "-" + Math.abs(UUID.randomUUID().hashCode()))
                    .append("speaker", currentSpeakerID)
                    .append("text", text)
                    .append("type", type);
            currentTextContentObjects.add(textDoc);
        }
    }

    private void finalizeCurrentSpeech() {
        if (currentSpeech != null) {
            // Add speaker information to the speech document
            String speakerName =
                    (currentSpeakerFirstName != null ? currentSpeakerFirstName + " " : "") +
                            (currentSpeakerLastName != null ? currentSpeakerLastName : "");

            if (!speakerName.isEmpty()) {
                currentSpeech.append("speakerName", speakerName.trim());
            }

            if (currentSpeakerID != null) {
                currentSpeech.append("speakerId", currentSpeakerID);
            }

            if (currentSpeakerParty != null) {
                currentSpeech.append("party", currentSpeakerParty);
            }

            // Add protocol information to the speech document
            if (currentProtocol != null) {
                currentSpeech.append("protocol", new Document(currentProtocol));
            }

            // Add agenda information to the speech document
            if (currentAgenda != null) {
                currentSpeech.append("agenda", new Document(currentAgenda));
            }

            // Process text content
            if (!currentTextContentObjects.isEmpty()) {
                // Filter objects of type "text", skipping the first object (index 0)
                List<Document> filteredTextObjects = new ArrayList<>();
                StringBuilder fullText = new StringBuilder();

                for (int i = 1; i < currentTextContentObjects.size(); i++) {  // Start from index 1
                    Document textObj = currentTextContentObjects.get(i);
                    if ("text".equals(textObj.getString("type"))) {
                        filteredTextObjects.add(textObj);
                        fullText.append(textObj.getString("text")).append(" ");
                    }
                }

                // Append only the filtered text objects
                currentSpeech.append("textContent", new ArrayList<>(currentTextContentObjects));
                currentSpeech.append("fullSpeechText", fullText.toString().trim());

                // Reset content objects for the next speech
                currentTextContentObjects.clear();
            }

            speeches.add(currentSpeech); // Add completed speech to list

            // Reset only the speech object for the next iteration
            currentSpeech = null;
        }

        inRede = false;  // Reset flag for next speech
    }

    private void finalizeProtocol() {
        if (!currentProtocol.containsKey("date")) {
            long defaultTimestamp = 0L;
            currentProtocol.append("date", defaultTimestamp)
                    .append("starttime", defaultTimestamp)
                    .append("endtime", defaultTimestamp);
        }

        if (!currentProtocol.containsKey("index")) {
            currentProtocol.append("index", 0);
        }

        if (!currentProtocol.containsKey("title")) {
            currentProtocol.append("title", null);
        }

        if (!currentProtocol.containsKey("place")) {
            currentProtocol.append("place", "Berlin");
        }

        if (!currentProtocol.containsKey("wp")) {
            currentProtocol.append("wp", 20); // Default Wahlperiode value
        }
    }

    private long parseDate(String dateString) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("dd.MM.yyyy");
            Date date = sdf.parse(dateString);
            return date.getTime();
        } catch (ParseException e) {
            return 0L; // Default value for invalid date parsing
        }
    }

    public List<Document> getAllSpeeches() {
        return speeches;
    }

    public List<Document> getAllAgendaItems() {
        return agendaItems;
    }

    public List<Document> getAllComments() {
        System.out.println("Total comments found: " + comments.size());
        return comments;
    }
}