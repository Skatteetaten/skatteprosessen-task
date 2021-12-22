package no.skatteetaten.fastsetting.formueinntekt.felles.task.api;

public enum TaskResult {

    SUCCESS(TaskState.SUCCEEDED),
    SUSPENSION(TaskState.SUSPENDED),
    FILTER(TaskState.FILTERED),
    FAILURE(TaskState.FAILED);

    private final TaskState state;

    TaskResult(TaskState state) {
        this.state = state;
    }

    public TaskState toState() {
        return state;
    }

    public TaskResult merge(TaskResult other) {
        switch (this) {
            case SUCCESS:
                return other;
            case SUSPENSION:
                return other == SUCCESS ? SUSPENSION : other;
            case FILTER:
                return other == FAILURE ? FAILURE : FILTER;
            case FAILURE:
                return FAILURE;
            default:
                throw new IllegalStateException();
        }
    }
}
