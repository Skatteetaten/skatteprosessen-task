package no.skatteetaten.fastsetting.formueinntekt.felles.task.api;

public enum TaskResult {

    FAILURE(TaskState.FAILED),
    SUSPENSION(TaskState.SUSPENDED),
    FILTER(TaskState.FILTERED),
    SUCCESS(TaskState.SUCCEEDED);

    private static final TaskResult[] VALUES = values();

    private final TaskState state;

    TaskResult(TaskState state) {
        this.state = state;
    }

    public static TaskResult ofOrdinal(int ordinal) {
        return VALUES[ordinal];
    }

    public TaskState toState() {
        return state;
    }

    public TaskResult merge(TaskResult other) {
        return ordinal() > other.ordinal() ? other : this;
    }
}
