package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.Task;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskDecision;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskSupplement;

public class CompositeTaskContext<TRANSACTION, EXCEPTION extends Exception, SUPPLEMENT extends TaskSupplement>
    extends TaskSupplement
    implements TaskContext<TRANSACTION, EXCEPTION, SUPPLEMENT> {

    private final Collection<TaskContext<? super TRANSACTION, ? extends EXCEPTION, ? super SUPPLEMENT>> delegates;

    @SafeVarargs
    public CompositeTaskContext(TaskContext<? super TRANSACTION, ? extends EXCEPTION, ? super SUPPLEMENT>... delegates) {
        this.delegates = Arrays.asList(delegates);
    }

    public CompositeTaskContext(Collection<TaskContext<? super TRANSACTION, ? extends EXCEPTION, ? super SUPPLEMENT>> delegates) {
        this.delegates = delegates;
    }

    @Override
    public CompletionStage<TaskCompletion<TRANSACTION, EXCEPTION>> apply(
        Map<Task, TaskDecision> decisions,
        Executor executor,
        SUPPLEMENT supplement
    ) {
        CompletionStage<TaskCompletion<TRANSACTION, EXCEPTION>> stage = CompletableFuture.completedStage(transaction -> decisions);
        try {
            for (TaskContext<? super TRANSACTION, ? extends EXCEPTION, ? super SUPPLEMENT> delegate : delegates) {
                stage = stage.thenCombineAsync(delegate.apply(decisions, executor, supplement).exceptionally(throwable -> transaction ->
                    decisions.keySet().stream().collect(Collectors.toMap(Function.identity(), task -> new TaskDecision(throwable)))
                ), (left, right) -> transaction -> Stream.concat(
                    left.complete(transaction).entrySet().stream(),
                    doFailMissingTasks(right.complete(transaction), decisions.keySet(), delegate).entrySet().stream()
                ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, TaskDecision::merge)), executor);
            }
        } catch (Throwable throwable) {
            stage = stage.thenApplyAsync(context -> transaction -> Stream.concat(
                context.complete(transaction).entrySet().stream(),
                decisions.keySet().stream().map(task -> Map.entry(task, new TaskDecision(throwable)))
            ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, TaskDecision::merge)), executor);
        }
        return stage;
    }

    @Override
    public void onAfterTransaction(Executor executor, SUPPLEMENT supplement) {
        delegates.forEach(delegate -> delegate.onAfterTransaction(executor, supplement));
    }

    private static Map<Task, TaskDecision> doFailMissingTasks(Map<Task, TaskDecision> decisions, Set<Task> tasks, TaskContext<?, ?, ?> context) {
        if (decisions.keySet().containsAll(tasks)) {
            return decisions;
        } else {
            return Stream.concat(decisions.keySet().stream(), tasks.stream().filter(task -> !decisions.containsKey(task))).collect(Collectors.toMap(
                Function.identity(), task -> decisions.getOrDefault(task, new TaskDecision(new IllegalStateException(
                    "Missing task decision in context " + context
                )))
            ));
        }
    }
}
