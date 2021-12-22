package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;

import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.Task;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskDecision;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskSupplement;

@FunctionalInterface
public interface TaskContext<TRANSACTION, EXCEPTION extends Exception> {

    default CompletionStage<TaskContextCompletion<TRANSACTION, EXCEPTION>> apply(
        Set<Task> tasks,
        TaskDecision decision
    ) throws EXCEPTION {
        return apply(tasks.stream().collect(Collectors.toMap(Function.identity(), task -> decision)));
    }

    default CompletionStage<TaskContextCompletion<TRANSACTION, EXCEPTION>> apply(
        Map<Task, TaskDecision> decisions
    ) throws EXCEPTION {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            return apply(decisions, executor).whenCompleteAsync((completion, throwable) -> executor.shutdown(), executor);
        } catch (Throwable t) {
            executor.shutdown();
            throw t;
        }
    }

    default CompletionStage<TaskContextCompletion<TRANSACTION, EXCEPTION>> apply(
        Map<Task, TaskDecision> decisions,
        Executor executor
    ) throws EXCEPTION {
        return apply(decisions, executor, new TaskSupplement());
    }

    CompletionStage<TaskContextCompletion<TRANSACTION, EXCEPTION>> apply(
        Map<Task, TaskDecision> decisions,
        Executor executor,
        TaskSupplement supplement
    ) throws EXCEPTION;

    default TaskContextFactory<TRANSACTION, EXCEPTION, TaskContext<TRANSACTION, EXCEPTION>> toFactory() {
        return (topic, tasks) -> this;
    }

    static <TRANSACTION, EXCEPTION extends Exception> TaskContext<TRANSACTION, EXCEPTION> simple() {
        return (decisions, executor, supplement) -> CompletableFuture.completedStage(transaction -> decisions);
    }
}
