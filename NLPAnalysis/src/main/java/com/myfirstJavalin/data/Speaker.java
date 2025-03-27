package com.myfirstJavalin.data;

import java.util.Date;
import java.util.List;

public interface Speaker {
    String getSpeakerID();
    void setSpeakerID(String speakerID);

    String getSpeakerLastName();
    void setSpeakerLastName(String speakerLastName);

    String getSpeakerFirstName();
    void setSpeakerFirstName(String speakerFirstName);

    String getTitle();
    void setTitle(String title);

    String getAcademicTitle();
    void setAcademicTitle(String academicTitle);

    String getGender();
    void setGender(String gender);

    String getBirthPlace();
    void setBirthPlace(String birthPlace);

    Date getDateOfBirth();

    Date getDateOfDeath();

    String getFamilyStatus();
    void setFamilyStatus(String familyStatus);

    String getReligion();
    void setReligion(String religion);

    String getJob();
    void setJob(String job);

    String getVita();

    void setVita(String vita);

    String getParty();
    void setParty(String party);




    String getFraction();
    void setFraction(String fraction);

    List<ElectionPeriod> getElectionPeriods();
}