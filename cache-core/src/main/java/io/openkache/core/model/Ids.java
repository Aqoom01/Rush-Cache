package io.openkache.core.model;

final class Ids {

    private Ids() {}

    static void requireNonNegative(String type, long value) {
        if (value < 0) {
            throw new IllegalArgumentException(type + " must be >= 0: " + value);
        }
    }
}
