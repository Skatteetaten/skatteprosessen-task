package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor;

import java.util.Map;

import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.Task;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskDecision;

@FunctionalInterface
public interface TaskCompletion<TRANSACTION, EXCEPTION extends Exception> {

    Map<Task, TaskDecision> complete(TRANSACTION transaction) throws EXCEPTION;
}
