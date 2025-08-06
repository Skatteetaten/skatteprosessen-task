package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor;

import java.util.Set;

import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.Task;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskSupplement;

@FunctionalInterface
public interface TaskContextFactory<TRANSACTION,
    EXCEPTION extends Exception,
    SUPPLEMENT extends TaskSupplement,
    CONTEXT extends TaskContext<TRANSACTION, EXCEPTION, ? super SUPPLEMENT>> {

    CONTEXT apply(String topic, Set<Task> tasks) throws EXCEPTION;
}
