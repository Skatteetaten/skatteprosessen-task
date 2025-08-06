package no.skatteetaten.fastsetting.formueinntekt.felles.task.api;

import java.util.*;
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
            TaskCreation... creations
    ) {
        pushByTask(task, topic, insertion, Arrays.asList(creations));
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

    default void junctionByTask(
        Task task,
        Set<String> identifiers,
        String topic,
        TaskJuncture.Junction junction
    ) {
        junctionByTask(Map.of(task, identifiers), topic, junction);
    }

    void junctionByTask(
        Map<Task, Set<String>> tasksToIdentifiers,
        String topic,
        TaskJuncture.Junction junction
    );
}
