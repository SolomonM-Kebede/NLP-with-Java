package com.myfirstJavalin.data;

import java.util.*;

public class SpeakerDBImpl implements Speaker {
    private String speakerID;
    private String speakerLastName;
    private String speakerFirstName;
    private String title;
    private String academicTitle;
    private String gender;
    private String birthPlace;
    private Date dateOfBirth;
    private Date dateOfDeath;
    private String familyStatus;
    private String religion;
    private String job;
    private String vita;
    private String party;
    private List<ElectionPeriod> electionPeriods;
    private String fraction;

    public SpeakerDBImpl() {
        this.electionPeriods = new ArrayList<>();
    }

    @Override
    public String getSpeakerID() {
        return speakerID;
    }

    @Override
    public void setSpeakerID(String speakerID) {
        this.speakerID = speakerID;
    }

    @Override
    public String getSpeakerLastName() {
        return speakerLastName;
    }

    @Override
    public void setSpeakerLastName(String speakerLastName) {
        this.speakerLastName = speakerLastName;
    }

    @Override
    public String getSpeakerFirstName() {
        return speakerFirstName;
    }

    @Override
    public void setSpeakerFirstName(String speakerFirstName) {
        this.speakerFirstName = speakerFirstName;
    }

    @Override
    public String getTitle() {
        return title;
    }

    @Override
    public void setTitle(String title) {
        this.title = title;
    }

    @Override
    public String getAcademicTitle() {
        return academicTitle;
    }

    @Override
    public void setAcademicTitle(String academicTitle) {
        this.academicTitle = academicTitle;
    }

    @Override
    public String getGender() {
        return gender;
    }

    @Override
    public void setGender(String gender) {
        this.gender = gender;
    }

    @Override
    public String getBirthPlace() {
        return birthPlace;
    }

    @Override
    public void setBirthPlace(String birthPlace) {
        this.birthPlace = birthPlace;
    }

    @Override
    public Date getDateOfBirth() {
        return dateOfBirth;
    }

    public void setDateOfBirth(Date dateOfBirth) {
        this.dateOfBirth = dateOfBirth;
    }

    @Override
    public Date getDateOfDeath() {
        return dateOfDeath;
    }

    public void setDateOfDeath(Date dateOfDeath) {
        this.dateOfDeath = dateOfDeath;
    }

    @Override
    public String getFamilyStatus() {
        return familyStatus;
    }

    @Override
    public void setFamilyStatus(String familyStatus) {
        this.familyStatus = familyStatus;
    }

    @Override
    public String getReligion() {
        return religion;
    }

    @Override
    public void setReligion(String religion) {
        this.religion = religion;
    }

    @Override
    public String getJob() {
        return job;
    }

    @Override
    public void setJob(String job) {
        this.job = job;
    }

    @Override
    public String getVita(){
        return vita;
    }

    @Override
    public void setVita(String vita){
        this.vita = vita;
    }

    @Override
    public String getParty() {
        return party;
    }

    @Override
    public void setParty(String party) {
        this.party = party;
    }




    @Override
    public String getFraction() {
        return fraction;
    }

    @Override
    public void setFraction(String fraction) {
        this.fraction = fraction;
    }

    /**
     */
    @Override
    public List<ElectionPeriod> getElectionPeriods() {
        return List.copyOf(electionPeriods);
    }

    @Override
    public String toString() {
        return "Speaker: " + speakerID + " - " + speakerFirstName + " " + speakerLastName;
    }

    public void setElectionPeriods(List<ElectionPeriod> electionPeriods) {
        if (electionPeriods != null) {
            this.electionPeriods = new ArrayList<>(electionPeriods);
        } else {
            this.electionPeriods = new ArrayList<>();
        }
    }
}