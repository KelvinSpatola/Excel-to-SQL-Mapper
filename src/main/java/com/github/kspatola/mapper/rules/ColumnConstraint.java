package com.github.kspatola.mapper.rules;

public enum ColumnConstraint {
    NOT_NULL("NOT NULL"), NULLABLE(""), PRIMARY_KEY("NOT NULL"), UNIQUE("UNIQUE"); // DEFAULT, CHECK

    private final String value;

    ColumnConstraint(String value) {
        this.value = value;
    }

    public String getValue() {
        return " " + value;
    }
}