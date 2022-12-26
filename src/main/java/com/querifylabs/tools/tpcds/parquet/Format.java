package com.querifylabs.tools.tpcds.parquet;

public enum Format {
    ORC("orc"),
    PARQUET("parquet");

    private final String formatName;

    Format(String formatName) {
        this.formatName = formatName;
    }

    public String path(String dir, String tableName) {
        return String.format("%s/%s/%s", dir, formatName, tableName);
    }

    public static Format resolve(String formatName) {
        for (var format : values()) {
            if (format.formatName.equalsIgnoreCase(formatName)) {
                return format;
            }
        }
        throw new IllegalArgumentException("Unknown format: " + formatName);
    }
}
