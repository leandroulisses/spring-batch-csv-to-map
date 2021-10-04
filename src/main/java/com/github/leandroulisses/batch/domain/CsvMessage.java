package com.github.leandroulisses.batch.domain;

public class CsvMessage {

    private String type;
    private String value;

    public void setType(String type) {
        this.type = type;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "CsvMessage{" +
                "type='" + type + '\'' +
                ", value='" + value + '\'' +
                '}';
    }

}
