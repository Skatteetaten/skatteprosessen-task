package no.skatteetaten.fastsetting.formueinntekt.felles.task.api;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TaskPartialBatch<SUPPLEMENT extends TaskSupplement> implements BiFunction<Set<Task>, SUPPLEMENT, CompletionStage<Map<Task, TaskDecision>>> {

    private final BiPredicate<Task, ? super SUPPLEMENT> predicate;
    private final BiFunction<Set<Task>, ? super SUPPLEMENT, CompletionStage<Map<Task, TaskDecision>>> delegate;

    private final TaskDecision fallback;

    public TaskPartialBatch(
        BiPredicate<Task, ? super SUPPLEMENT> predicate,
        BiFunction<Set<Task>, ? super SUPPLEMENT, CompletionStage<Map<Task, TaskDecision>>> delegate
    ) {
        this(predicate, delegate, TaskDecision.SUCCESS);
    }

    public TaskPartialBatch(
        BiPredicate<Task, ? super SUPPLEMENT> predicate,
        BiFunction<Set<Task>, ? super SUPPLEMENT, CompletionStage<Map<Task, TaskDecision>>> delegate,
        TaskDecision fallback
    ) {
        this.predicate = predicate;
        this.delegate = delegate;
        this.fallback = fallback;
    }

    public static <SUPPLEMENT extends TaskSupplement> BiFunction<Set<Task>, SUPPLEMENT, CompletionStage<Map<Task, TaskDecision>>> ofNoResult(
        BiPredicate<Task, SUPPLEMENT> predicate,
        BiConsumer<Set<Task>, SUPPLEMENT> delegate
    ) {
        return of(predicate, delegate);
    }

    public static <SUPPLEMENT extends TaskSupplement> BiFunction<Set<Task>, SUPPLEMENT, CompletionStage<Map<Task, TaskDecision>>> ofNoResult(
        BiPredicate<Task, ? super SUPPLEMENT> predicate,
        BiConsumer<Set<Task>, ? super SUPPLEMENT> delegate,
        TaskDecision fallback
    ) {
        return of(predicate, delegate, fallback);
    }

    public static <SUPPLEMENT extends TaskSupplement> BiFunction<Set<Task>, SUPPLEMENT, CompletionStage<Map<Task, TaskDecision>>> of(
        BiPredicate<Task, ? super SUPPLEMENT> predicate,
        BiConsumer<Set<Task>, ? super SUPPLEMENT> delegate
    ) {
        return of(predicate, delegate, TaskDecision.SUCCESS);
    }

    public static <SUPPLEMENT extends TaskSupplement> BiFunction<Set<Task>, SUPPLEMENT, CompletionStage<Map<Task, TaskDecision>>> of(
        BiPredicate<Task, ? super SUPPLEMENT> predicate,
        BiConsumer<Set<Task>, ? super SUPPLEMENT> delegate,
        TaskDecision fallback
    ) {
        return of(predicate, (tasks, supplement) -> {
            delegate.accept(tasks, supplement);
            return tasks.stream().collect(Collectors.toMap(Function.identity(), task -> TaskDecision.SUCCESS));
        }, fallback);
    }

    public static <SUPPLEMENT extends TaskSupplement> BiFunction<Set<Task>, SUPPLEMENT, CompletionStage<Map<Task, TaskDecision>>> ofResult(
        BiPredicate<Task, ? super SUPPLEMENT> predicate,
        BiFunction<Set<Task>, ? super SUPPLEMENT, Map<Task, TaskDecision>> delegate
    ) {
        return of(predicate, delegate);
    }

    public static <SUPPLEMENT extends TaskSupplement> BiFunction<Set<Task>, SUPPLEMENT, CompletionStage<Map<Task, TaskDecision>>> ofResult(
        BiPredicate<Task, ? super SUPPLEMENT> predicate,
        BiFunction<Set<Task>, ? super SUPPLEMENT, Map<Task, TaskDecision>> delegate,
        TaskDecision fallback
    ) {
        return of(predicate, delegate, fallback);
    }

    public static <SUPPLEMENT extends TaskSupplement> BiFunction<Set<Task>, SUPPLEMENT, CompletionStage<Map<Task, TaskDecision>>> of(
        BiPredicate<Task, ? super SUPPLEMENT> predicate,
        BiFunction<Set<Task>, ? super SUPPLEMENT, Map<Task, TaskDecision>> delegate
    ) {
        return of(predicate, delegate, TaskDecision.SUCCESS);
    }

    public static <SUPPLEMENT extends TaskSupplement> BiFunction<Set<Task>, SUPPLEMENT, CompletionStage<Map<Task, TaskDecision>>> of(
        BiPredicate<Task, ? super SUPPLEMENT> predicate,
        BiFunction<Set<Task>, ? super SUPPLEMENT, Map<Task, TaskDecision>> delegate,
        TaskDecision fallback
    ) {
        return new TaskPartialBatch<>(predicate, delegate.andThen(CompletableFuture::completedStage), fallback);
    }

    @Override
    public CompletionStage<Map<Task, TaskDecision>> apply(Set<Task> tasks, SUPPLEMENT supplement) {
        Set<Task> active = tasks.stream().filter(task -> predicate.test(task, supplement)).collect(Collectors.toCollection(LinkedHashSet::new));
        CompletionStage<Map<Task, TaskDecision>> stage = active.isEmpty()
            ? CompletableFuture.completedStage(Collections.emptyMap())
            : delegate.apply(active, supplement);
        return stage.handle((decisions, throwable) -> {
            Map<Task, TaskDecision> realized;
            if (throwable != null) {
                realized = active.stream().collect(Collectors.toMap(Function.identity(), task -> new TaskDecision(throwable)));
            } else {
                realized = decisions;
            }
            return (throwable == null ? Stream.concat(
                tasks.stream(),
                decisions.keySet().stream().filter(task -> !tasks.contains(task))
            ) : tasks.stream()).collect(Collectors.toMap(Function.identity(), task -> realized.getOrDefault(task, fallback)));
        });
    }
}
