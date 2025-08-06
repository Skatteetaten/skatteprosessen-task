package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor;

public class TaskHandlerShutdownException extends RuntimeException {

    private static final long serialVersionUID = -1;

    public TaskHandlerShutdownException() {
        super("Shutting down task handler", null, false, false);
    }
}
