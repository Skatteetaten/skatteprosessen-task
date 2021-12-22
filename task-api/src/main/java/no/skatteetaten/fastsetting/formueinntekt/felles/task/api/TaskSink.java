package no.skatteetaten.fastsetting.formueinntekt.felles.task.api;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public interface TaskSink<TRANSACTION, EXCEPTION extends Exception> {

    default Task push(
        TRANSACTION transaction, String topic, Insertion insertion, String identifier
    ) throws EXCEPTION {
        return push(transaction, topic, insertion, identifier, "");
    }

    default Task push(
        TRANSACTION transaction, String topic, Insertion insertion, String identifier, String input
    ) throws EXCEPTION {
        return push(transaction, topic, insertion, Collections.singletonMap(identifier, input)).get(0);
    }

    default List<Task> push(
        TRANSACTION transaction, String topic, Insertion insertion, String... identifiers
    ) throws EXCEPTION {
        return push(transaction, topic, insertion, Arrays.stream(identifiers).collect(Collectors.toMap(Function.identity(), identifier -> "")));
    }

    default List<Task> push(
        TRANSACTION transaction, String topic, Insertion insertion, Map<String, String> creations
    ) throws EXCEPTION {
        return push(transaction, topic, insertion, creations.entrySet().stream()
            .map(entry -> new TaskCreation(entry.getKey(), entry.getValue()))
            .collect(Collectors.toList()));
    }

    List<Task> push(
        TRANSACTION transaction, String topic, Insertion insertion, Collection<TaskCreation> creations
    ) throws EXCEPTION;

    enum Insertion {
        APPEND,
        REPLACE
    }
}
