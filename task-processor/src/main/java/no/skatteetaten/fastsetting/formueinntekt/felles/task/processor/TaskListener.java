package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor;

import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.Task;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskDecision;

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

public interface TaskListener<TRACE> {

    void onFatal(String topic, Throwable throwable);

    void onEmpty(String topic);

    TRACE onStart(String topic, Set<Task> tasks);

    void onDispatched(String topic, TRACE trace, Set<Task> tasks);

    void onCallback(String topic, TRACE trace, Set<Task> tasks);

    void onComplete(String topic, TRACE trace, Map<Task, TaskDecision> decisions);

    void onError(String topic, TRACE trace, Set<Task> tasks, Throwable throwable);

    static TaskListener<?> noop() {
        return onFatal(throwable -> { });
    }

    static TaskListener<?> onFatal(Consumer<Throwable> consumer) {
        return new TaskListener<>() {
            @Override
            public void onFatal(String topic, Throwable throwable) {
                consumer.accept(throwable);
            }

            @Override
            public void onEmpty(String topic) { }

            @Override
            public Object onStart(String topic, Set<Task> tasks) {
                return null;
            }

            @Override
            public void onDispatched(String topic, Object trace, Set<Task> tasks) { }

            @Override
            public void onCallback(String topic, Object trace, Set<Task> tasks) { }

            @Override
            public void onComplete(String topic, Object trace, Map<Task, TaskDecision> decisions) { }

            @Override
            public void onError(String topic, Object trace, Set<Task> tasks, Throwable throwable) { }
        };
    }
}
