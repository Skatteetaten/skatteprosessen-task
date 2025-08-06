package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.Task;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskDecision;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskSupplement;

@FunctionalInterface
public interface TaskContext<TRANSACTION, EXCEPTION extends Exception, SUPPLEMENT extends TaskSupplement> {

    CompletionStage<TaskCompletion<TRANSACTION, EXCEPTION>> apply(
        Map<Task, TaskDecision> decisions,
        Executor executor,
        SUPPLEMENT supplement
    ) throws EXCEPTION;

    default void onAfterTransaction(Executor executor, SUPPLEMENT supplement) { }

    default TaskContextFactory<TRANSACTION, EXCEPTION, SUPPLEMENT, TaskContext<TRANSACTION, EXCEPTION, SUPPLEMENT>> toFactory() {
        return (topic, tasks) -> this;
    }

    static <TRANSACTION, EXCEPTION extends Exception, SUPPLEMENT extends TaskSupplement> TaskContext<TRANSACTION, EXCEPTION, SUPPLEMENT> simple() {
        return (decisions, executor, supplement) -> CompletableFuture.completedStage(transaction -> decisions);
    }
}
