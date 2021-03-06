package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor;

import java.util.Map;

import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.Task;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskDecision;

@FunctionalInterface
public interface TaskContextCompletion<TRANSACTION, EXCEPTION extends Exception> {

    Map<Task, TaskDecision> complete(TRANSACTION transaction) throws EXCEPTION;

    default TaskCompletion<TRANSACTION, EXCEPTION> toCompletion() {
        return this::complete;
    }
}
