package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor;

import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.Task;

import java.util.Collection;
import java.util.Set;

@FunctionalInterface
public interface TaskContextFactory<TRANSACTION, EXCEPTION extends Exception, CONTEXT extends TaskContext<TRANSACTION, EXCEPTION>> {

    CONTEXT apply(String topic, Set<Task> tasks) throws EXCEPTION;
}
