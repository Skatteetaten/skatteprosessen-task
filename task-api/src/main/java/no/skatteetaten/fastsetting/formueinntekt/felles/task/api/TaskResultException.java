package no.skatteetaten.fastsetting.formueinntekt.felles.task.api;

public class TaskResultException extends RuntimeException {

    private final TaskResult result;
    private final boolean continued;

    public TaskResultException(TaskResult result) {
        this.result = result;
        continued = result == TaskResult.SUCCESS;
    }

    public TaskResultException(TaskResult result, boolean continued) {
        this.result = result;
        this.continued = continued;
    }

    public TaskResultException(String message) {
        super(message);
        result = TaskResult.FAILURE;
        continued = false;
    }

    public TaskResultException(Throwable cause) {
        super(cause);
        result = TaskResult.FAILURE;
        continued = false;
    }

    public TaskResultException(TaskResult result, Throwable cause) {
        super(cause);
        this.result = result;
        continued = result == TaskResult.SUCCESS;
    }

    public TaskResultException(TaskResult result, String message) {
        super(message);
        this.result = result;
        continued = result == TaskResult.SUCCESS;
    }

    public TaskResultException(TaskResult result, Throwable cause, boolean continued) {
        super(cause);
        this.result = result;
        this.continued = continued;
    }

    public TaskResultException(TaskResult result, String message, boolean continued) {
        super(message);
        this.result = result;
        this.continued = continued;
    }

    public TaskResult getResult() {
        return result;
    }

    public TaskDecision toDecision() {
        TaskDecision decision = getCause() == null
            ? new TaskDecision(result, getMessage())
            : new TaskDecision(result, getCause());
        return decision.withContinuation(continued);
    }
}
