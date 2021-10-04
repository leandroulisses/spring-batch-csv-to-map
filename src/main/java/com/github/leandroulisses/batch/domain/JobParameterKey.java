package com.github.leandroulisses.batch.domain;

public enum JobParameterKey {
    TEMP_FILE_LOCATION("fileLocation"),
    IMPORT_ID("importId");

    private String value;

    public String getValue() {
        return value;
    }

    JobParameterKey(String value) {
        this.value = value;
    }
}
