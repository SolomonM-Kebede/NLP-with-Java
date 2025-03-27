package com.myfirstJavalin.data;

import org.jetbrains.annotations.NotNull;
import org.json.JSONObject;


import java.util.List;
import java.util.Objects;

/**
 * Interface representing a Speech.
 * Defines the contract for a Speech object, including its properties and methods for manipulation and transport.
 */
public interface Speech extends Comparable<Speech> {

    /**
     * Gets the unique identifier for the speech.
     *
     * @return The speech ID.
     */
    String getSpeechID();
    void setSpeechID(String speechID);


    String getFullSpeechText();

    void setFullSpeechText(String FullSpeechText);

    /**
     * Gets the unique identifier for the speaker of the speech.
     *
     * @return The speaker's ID.
     */
    String getSpeakerID();
    void setSpeakerID(String speakerID);

    /**
     * Gets the protocol related to the speech.
     * The protocol can be any complex object, so consider specifying the exact type for better clarity.
     *
     * @return The protocol object.
     */
    Object getProtocol();
    void setProtocol(Object protocol);

    /**
     * Gets the list of text content of the speech. Assumed to be a list of strings.
     *
     * @return The list of text content.
     */
    List<Objects> getTextContent();
    void setTextContent(List<Objects> textContent);

    /**
     * Gets the agenda related to the speech.
     * The agenda is assumed to be a list of strings.
     *
     * @return The agenda list.
     */
    Object getAgenda();
    void setAgenda(Object agenda);

    List<Objects> getComments();
    void setComments(List<Objects> comments);

    /**
     * Converts the speech to a JSON object for easy transport to the front-end.
     *
     * @return The speech data in JSON format.
     */
    JSONObject toJSON();


    /**
     * Compares this speech with another based on the speech ID.
     *
     * @param other The other speech to compare to.
     * @return A negative integer, zero, or a positive integer as this speech's ID is less than, equal to, or greater than the other speech's ID.
     */
    @Override
    int compareTo(@NotNull Speech other);

}
