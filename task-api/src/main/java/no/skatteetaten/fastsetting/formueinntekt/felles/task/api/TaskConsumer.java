package no.skatteetaten.fastsetting.formueinntekt.felles.task.api;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.function.Function;

public interface TaskConsumer {

    default void pushByTask(
        Task task,
        String topic,
        TaskSink.Insertion insertion
    ) {
        pushByTask(Collections.singleton(task), topic, insertion);
    }

    default void pushByTask(
        Task task,
        String topic,
        TaskSink.Insertion insertion,
        Collection<TaskCreation> creations
    ) {
        pushByTask(Collections.singleton(task), topic, insertion, ignored -> creations);
    }

    default void pushByTask(
        Set<Task> tasks,
        String topic,
        TaskSink.Insertion insertion
    ) {
        pushByTask(
            tasks, topic, insertion,
            task -> Collections.singleton(new TaskCreation(task.getIdentifier()))
        );
    }

    void pushByTask(
        Set<Task> tasks,
        String topic,
        TaskSink.Insertion insertion,
        Function<Task, Collection<TaskCreation>> resolver
    );
}
