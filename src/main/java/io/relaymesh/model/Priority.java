package io.relaymesh.model;

public enum Priority {
    HIGH("high"),
    NORMAL("normal"),
    LOW("low");

    private final String dirName;

    Priority(String dirName) {
        this.dirName = dirName;
    }

    public String dirName() {
        return dirName;
    }

    public static Priority fromString(String raw) {
        if (raw == null || raw.isBlank()) {
            return NORMAL;
        }
        for (Priority value : values()) {
            if (value.name().equalsIgnoreCase(raw) || value.dirName.equalsIgnoreCase(raw)) {
                return value;
            }
        }
        throw new IllegalArgumentException("Unknown priority: " + raw);
    }
}

