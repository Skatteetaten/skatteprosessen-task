package no.skatteetaten.fastsetting.formueinntekt.felles.task.api;

public enum TaskState {

    ACTIVE,
    READY,
    EXPIRED,
    SUCCEEDED,
    SUSPENDED,
    FILTERED,
    FAILED,
    RECREATED,
    REDUNDANT;

    private static final TaskState[] VALUES = values();

    public static TaskState ofOrdinal(int ordinal) {
        return VALUES[ordinal];
    }
}
