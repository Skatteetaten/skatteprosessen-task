package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor;

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.Task;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskDecision;

@FunctionalInterface
public interface TaskHandler<TRANSACTION, EXCEPTION extends Exception> extends AutoCloseable {

    void accept(
        Set<Task> tasks,
        TaskCallback<TRANSACTION, EXCEPTION> callback,
        Consumer<Throwable> onFailure
    ) throws EXCEPTION;

    @Override
    default void close() {
        /* do nothing */
    }

    default Supplier<TaskHandler<TRANSACTION, EXCEPTION>> toFactory() {
        return () -> this;
    }

    static <TRANSACTION, EXCEPTION extends Exception> TaskHandler<TRANSACTION, EXCEPTION> simple(Function<Task, TaskDecision> resolver) {
        return (tasks, callback, onFailure) -> {
            Map<Task, TaskDecision> decisions = tasks.stream().collect(Collectors.toMap(Function.identity(), resolver));
            callback.accept(transaction -> decisions);
        };
    }
}
