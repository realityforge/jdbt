package org.realityforge.jdbt.repository;

public enum RowSource {
    IMPORT("import"),
    DEPLOYMENT("deployment");

    private final String externalValue;

    RowSource(final String externalValue) {
        this.externalValue = externalValue;
    }

    public String externalValue() {
        return externalValue;
    }
}
