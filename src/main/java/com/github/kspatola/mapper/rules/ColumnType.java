package com.github.kspatola.mapper.rules;

public enum ColumnType {
    BOOLEAN("BOOLEAN"), VARCHAR("VARCHAR(100)"), INT("INT"), DOUBLE("DOUBLE"), DATE("DATE"), TIME("TIME");

    private final String value;

    ColumnType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}