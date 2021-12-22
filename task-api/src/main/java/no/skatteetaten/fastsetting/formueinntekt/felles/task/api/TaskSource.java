package no.skatteetaten.fastsetting.formueinntekt.felles.task.api;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface TaskSource<TRANSACTION, EXCEPTION extends Exception> {

    default Optional<Task> poll(
        TRANSACTION transaction, String topic
    ) throws EXCEPTION {
        return poll(transaction, topic, 1).stream().findFirst();
    }

    Set<Task> poll(
        TRANSACTION transaction, String topic, int size
    ) throws EXCEPTION;

    default void complete(
        TRANSACTION transaction, String topic, Task task, TaskDecision decision
    ) throws EXCEPTION {
        complete(transaction, topic, Collections.singletonMap(task, decision));
    }

    void complete(
        TRANSACTION transaction, String topic, Map<Task, TaskDecision> decisions
    ) throws EXCEPTION;
}
