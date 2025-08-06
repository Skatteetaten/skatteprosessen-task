package no.skatteetaten.fastsetting.formueinntekt.felles.task.api;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public interface TaskSink<TRANSACTION, EXCEPTION extends Exception> {

    default Task push(
        TRANSACTION transaction, String topic, Insertion insertion, String identifier
    ) throws EXCEPTION {
        return push(transaction, topic, insertion, new TaskCreation(identifier));
    }

    default Task push(
        TRANSACTION transaction, String topic, Insertion insertion, TaskCreation creation
    ) throws EXCEPTION {
        return push(transaction, topic, insertion, Collections.singletonList(creation)).get(0);
    }

    default List<Task> push(
        TRANSACTION transaction, String topic, Insertion insertion, String... identifiers
    ) throws EXCEPTION {
        return push(transaction, topic, insertion, Arrays.stream(identifiers)
            .map(TaskCreation::new)
            .collect(Collectors.toList()));
    }

    default List<Task> push(
        TRANSACTION transaction, String topic, Insertion insertion, Map<String, String> creations
    ) throws EXCEPTION {
        return push(transaction, topic, insertion, creations.entrySet().stream()
            .map(entry -> new TaskCreation(entry.getKey(), entry.getValue()))
            .collect(Collectors.toList()));
    }

    default List<Task> push(
        TRANSACTION transaction, String topic, Insertion insertion, TaskCreation... creations
    ) throws EXCEPTION {
        return push(transaction, topic, insertion, Arrays.asList(creations));
    }

    List<Task> push(
        TRANSACTION transaction, String topic, Insertion insertion, Collection<TaskCreation> creations
    ) throws EXCEPTION;

    enum Insertion {
        APPEND,
        SUPERSEDE,
        REPLACE,
        DELETE
    }
}
