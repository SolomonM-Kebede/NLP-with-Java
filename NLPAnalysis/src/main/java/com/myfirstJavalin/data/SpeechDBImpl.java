package com.myfirstJavalin.data;

import org.bson.Document;
import org.jetbrains.annotations.NotNull;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Implements the {@link Speech} interface for MongoDB-backed Speech data.
 * The data is represented by a {@link Document} from MongoDB, and methods
 * are provided to access and convert this data.
 */
public class SpeechDBImpl implements Speech {
    private String speechID;
    private String speakerName;
    private  String speakerID;
    private List<Objects> protocol;
    private List<Objects> textContents = new ArrayList<>();
    private List<Objects> agenda;
    private final Document parliamentDocument;

    /**
     * Constructor to initialize the {@link SpeechDBImpl} with a MongoDB document.
     *
     * @param parliamentDocument The MongoDB document containing speech data.
     */
    public SpeechDBImpl(Document parliamentDocument) {
        this.parliamentDocument = parliamentDocument;
    }

    @Override
    public String getSpeechID() {
        return parliamentDocument.getString(speechID);
    }

    /**
     * @param speechID
     */
    @Override
    public void setSpeechID(String speechID) {this.speechID = speechID;}



    @Override
    public String getFullSpeechText() {
        return parliamentDocument.getString("FullSpeechText");
    }

    /**
     * @param FullSpeechText
     */
    @Override
    public void setFullSpeechText(String FullSpeechText) {

    }

    @Override
    public String getSpeakerID() {
        return parliamentDocument.getString("speaker");
    }

    /**
     * @param speakerID
     */
    @Override
    public void setSpeakerID(String speakerID) {

    }

    @Override
    public Object getProtocol() {
        return parliamentDocument.get("protocol"); // Specify protocol type if possible
    }

    /**
     * @param protocol
     */
    @Override
    public void setProtocol(Object protocol) {

    }

    /**
     * Retrieves the text content of the speech.
     * Assumes that the "textContent" field in the MongoDB document is a list.
     *
     * @return A list containing text content, or an empty list if not found or invalid.
     */
    @Override
    public List<Objects> getTextContent() {
        Object textContent = parliamentDocument.get("textContent");
        if (textContent instanceof List<?>) {
            // Check if it's a List of String objects
            return (List<Objects>) textContent;
        } else {
            // Return an empty list or handle the error
            return List.of(); // Return an empty list if the field isn't a List
        }
    }

    /**
     * @param textContent
     */
    @Override
    public void setTextContent(List<Objects> textContent) {

    }

    @Override
    public Object getAgenda() {
        return parliamentDocument.get("agenda");
    }

    /**
     * @param agenda
     */
    @Override
    public void setAgenda(Object agenda) {

    }

    @Override
    public List<Objects> getComments() {
        Object comments = parliamentDocument.get("comments");
        if (comments instanceof List<?>) {
            return (List<Objects>) comments;

        }else
            return List.of();
    }

    /**
     * @param comments
     */
    @Override
    public void setComments(List<Objects> comments) {

    }

    /**
     * Converts the {@link SpeechDBImpl} object to a {@link JSONObject} for easy transport.
     *
     * @return The {@link JSONObject} representation of the speech.
     */
    @Override
    public JSONObject toJSON() {
        JSONObject speechObj = new JSONObject();
        speechObj.put("_id", getSpeechID());
        speechObj.put("fullSpeechText", getFullSpeechText());
        speechObj.put("speaker", getSpeakerID());
        speechObj.put("protocol", getProtocol());
        speechObj.put("textContent", getTextContent());
        speechObj.put("agenda", getAgenda());
        speechObj.put("comments", getComments());
        return speechObj;
    }

    /**
     * Provides a string representation of the SpeechDB_Impl object for debugging or logging.
     *
     * @return The string representation of the speech, including its ID and text.
     */
    @Override
    public String toString() {
        return this.getSpeechID() + " " + this.getFullSpeechText();
    }

    /**
     * Compares this speech with another speech based on the speech ID.
     *
     * @param o The other speech to compare to.
     * @return A negative integer, zero, or a positive integer as this speech's ID is
     *         less than, equal to, or greater than the other speech's ID.
     */
    @Override
    public int compareTo(@NotNull Speech o) {
        return this.getSpeechID().compareTo(o.getSpeechID());
    }

    /**
     * Calculates a hash code for the {@link SpeechDBImpl} object based on its essential properties.
     *
     * @return The hash code.
     */
    @Override
    public int hashCode() {
        return Objects.hash(getSpeechID(), getFullSpeechText(), getSpeakerID());
    }

    /**
     * Compares this {@link SpeechDBImpl} object with another object for equality.
     *
     * @param obj The object to compare to.
     * @return {@code true} if the objects are equal, otherwise {@code false}.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        SpeechDBImpl speech = (SpeechDBImpl) obj;
        return Objects.equals(getSpeechID(), speech.getSpeechID()) &&
                Objects.equals(getFullSpeechText(), speech.getFullSpeechText()) &&
                Objects.equals(getSpeakerID(), speech.getSpeakerID());
    }

    public List<Objects> getTextContents() {
        return textContents;
    }

    public void setTextContents(List<Objects> textContents) {
        this.textContents = textContents;
    }
}