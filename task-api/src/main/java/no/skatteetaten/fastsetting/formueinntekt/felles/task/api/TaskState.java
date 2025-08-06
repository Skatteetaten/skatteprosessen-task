package no.skatteetaten.fastsetting.formueinntekt.felles.task.api;

public enum TaskState {

    ACTIVE,
    READY,
    EXPIRED,
    FAILED,
    SUSPENDED,
    FILTERED,
    SUCCEEDED,
    RECREATED,
    REDUNDANT;

    private static final TaskState[] VALUES = values();

    public static TaskState ofOrdinal(int ordinal) {
        return VALUES[ordinal];
    }
}
