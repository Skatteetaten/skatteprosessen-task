package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.Task;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskDecision;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskResult;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskResultException;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskSupplement;

public class CompositeTaskHandlerFactory<TRANSACTION,
    EXCEPTION extends Exception,
    IDENTITY,
    SUPPLEMENT extends TaskSupplement,
    CONTEXT extends TaskContext<TRANSACTION, EXCEPTION, SUPPLEMENT>> implements Function<String, TaskHandler<TRANSACTION, EXCEPTION>> {

    private final Executor executor;

    private final boolean failPendingOnClose;
    private final int concurrency;

    private final TaskContextFactory<TRANSACTION, EXCEPTION, ? super SUPPLEMENT, CONTEXT> contextFactory;
    private final Function<CONTEXT, ? extends SUPPLEMENT> supplementFactory;
    private final Function<String, BiFunction<IDENTITY,
        BiFunction<Set<Task>, ? super SUPPLEMENT, CompletionStage<Map<Task, TaskDecision>>>,
        BiFunction<Set<Task>, ? super SUPPLEMENT, CompletionStage<Map<Task, TaskDecision>>>>> decoratorFactory;

    private final Map<IDENTITY, Registration<SUPPLEMENT, IDENTITY>> registrations;

    public CompositeTaskHandlerFactory(
        Executor executor,
        boolean failPendingOnClose,
        int concurrency,
        TaskContextFactory<TRANSACTION, EXCEPTION, ? super SUPPLEMENT, CONTEXT> contextFactory,
        Function<CONTEXT, ? extends SUPPLEMENT> supplementFactory
    ) {
        this.executor = executor;
        this.failPendingOnClose = failPendingOnClose;
        this.concurrency = concurrency;
        this.supplementFactory = supplementFactory;
        this.contextFactory = contextFactory;
        decoratorFactory = topic -> (identity, step) -> step;
        registrations = Collections.emptyMap();
    }

    public CompositeTaskHandlerFactory(
        Executor executor,
        boolean failPendingOnClose,
        int concurrency,
        TaskContextFactory<TRANSACTION, EXCEPTION, ? super SUPPLEMENT, CONTEXT> contextFactory,
        Function<CONTEXT, ? extends SUPPLEMENT> supplementFactory,
        Function<String, BiFunction<IDENTITY,
            BiFunction<Set<Task>, ? super SUPPLEMENT, CompletionStage<Map<Task, TaskDecision>>>,
            BiFunction<Set<Task>, ? super SUPPLEMENT, CompletionStage<Map<Task, TaskDecision>>>>> decoratorFactory
    ) {
        this.executor = executor;
        this.failPendingOnClose = failPendingOnClose;
        this.concurrency = concurrency;
        this.supplementFactory = supplementFactory;
        this.contextFactory = contextFactory;
        this.decoratorFactory = decoratorFactory;
        registrations = Collections.emptyMap();
    }

    private CompositeTaskHandlerFactory(
        Executor executor,
        boolean failPendingOnClose,
        int concurrency,
        TaskContextFactory<TRANSACTION, EXCEPTION, ? super SUPPLEMENT, CONTEXT> contextFactory,
        Function<CONTEXT, ? extends SUPPLEMENT> supplementFactory,
        Function<String, BiFunction<IDENTITY,
            BiFunction<Set<Task>, ? super SUPPLEMENT, CompletionStage<Map<Task, TaskDecision>>>,
            BiFunction<Set<Task>, ? super SUPPLEMENT, CompletionStage<Map<Task, TaskDecision>>>>> decoratorFactory,
        Map<IDENTITY, Registration<SUPPLEMENT, IDENTITY>> registrations
    ) {
        this.executor = executor;
        this.failPendingOnClose = failPendingOnClose;
        this.concurrency = concurrency;
        this.supplementFactory = supplementFactory;
        this.contextFactory = contextFactory;
        this.decoratorFactory = decoratorFactory;
        this.registrations = registrations;
    }

    @SafeVarargs
    public final CompositeTaskHandlerFactory<TRANSACTION, EXCEPTION, IDENTITY, SUPPLEMENT, CONTEXT> withEach(
        IDENTITY identity,
        BiConsumer<Task, ? super SUPPLEMENT> step,
        IDENTITY... dependencies
    ) {
        return withEachNoResult(identity, step, dependencies);
    }

    @SafeVarargs
    public final CompositeTaskHandlerFactory<TRANSACTION, EXCEPTION, IDENTITY, SUPPLEMENT, CONTEXT> withEach(
        IDENTITY identity,
        Mode mode,
        BiConsumer<Task, ? super SUPPLEMENT> step,
        IDENTITY... dependencies
    ) {
        return withEachNoResult(identity, mode, step, dependencies);
    }

    @SafeVarargs
    public final CompositeTaskHandlerFactory<TRANSACTION, EXCEPTION, IDENTITY, SUPPLEMENT, CONTEXT> withEach(
        IDENTITY identity,
        BiFunction<Task, ? super SUPPLEMENT, TaskDecision> step,
        IDENTITY... dependencies
    ) {
        return withEachResult(identity, step, dependencies);
    }

    @SafeVarargs
    public final CompositeTaskHandlerFactory<TRANSACTION, EXCEPTION, IDENTITY, SUPPLEMENT, CONTEXT> withEach(
        IDENTITY identity,
        Mode mode,
        BiFunction<Task, ? super SUPPLEMENT, TaskDecision> step,
        IDENTITY... dependencies
    ) {
        return withEachResult(identity, mode, step, dependencies);
    }

    @SafeVarargs
    public final CompositeTaskHandlerFactory<TRANSACTION, EXCEPTION, IDENTITY, SUPPLEMENT, CONTEXT> with(
        IDENTITY identity,
        BiConsumer<Set<Task>, ? super SUPPLEMENT> step,
        IDENTITY... dependencies
    ) {
        return withNoResult(identity, step, dependencies);
    }

    @SafeVarargs
    public final CompositeTaskHandlerFactory<TRANSACTION, EXCEPTION, IDENTITY, SUPPLEMENT, CONTEXT> with(
        IDENTITY identity,
        BiFunction<Set<Task>, ? super SUPPLEMENT, Map<Task, TaskDecision>> step,
        IDENTITY... dependencies
    ) {
        return withResult(identity, step, dependencies);
    }

    @SafeVarargs
    public final CompositeTaskHandlerFactory<TRANSACTION, EXCEPTION, IDENTITY, SUPPLEMENT, CONTEXT> withEachNoResult(
        IDENTITY identity,
        BiConsumer<Task, ? super SUPPLEMENT> step,
        IDENTITY... dependencies
    ) {
        return withEachNoResult(identity, Mode.PARALLEL, step, dependencies);
    }

    @SafeVarargs
    public final CompositeTaskHandlerFactory<TRANSACTION, EXCEPTION, IDENTITY, SUPPLEMENT, CONTEXT> withEachNoResult(
        IDENTITY identity,
        Mode mode,
        BiConsumer<Task, ? super SUPPLEMENT> step,
        IDENTITY... dependencies
    ) {
        return withEachResult(identity, mode, (task, supplement) -> {
            step.accept(task, supplement);
            return TaskDecision.SUCCESS;
        }, dependencies);
    }

    @SafeVarargs
    public final CompositeTaskHandlerFactory<TRANSACTION, EXCEPTION, IDENTITY, SUPPLEMENT, CONTEXT> withEachResult(
        IDENTITY identity,
        BiFunction<Task, ? super SUPPLEMENT, TaskDecision> step,
        IDENTITY... dependencies
    ) {
        return withEachResult(identity, Mode.PARALLEL, step, dependencies);
    }

    @SafeVarargs
    public final CompositeTaskHandlerFactory<TRANSACTION, EXCEPTION, IDENTITY, SUPPLEMENT, CONTEXT> withEachResult(
        IDENTITY identity,
        Mode mode,
        BiFunction<Task, ? super SUPPLEMENT, TaskDecision> step,
        IDENTITY... dependencies
    ) {
        return withEachAsync(
            identity,
            mode,
            (task, supplement) -> CompletableFuture.completedStage(step.apply(task, supplement)),
            dependencies
        );
    }

    @SafeVarargs
    public final CompositeTaskHandlerFactory<TRANSACTION, EXCEPTION, IDENTITY, SUPPLEMENT, CONTEXT> withNoResult(
        IDENTITY identity,
        BiConsumer<Set<Task>, ? super SUPPLEMENT> step,
        IDENTITY... dependencies
    ) {
        return withResult(identity, (tasks, supplement) -> {
            step.accept(tasks, supplement);
            return tasks.stream().collect(Collectors.toMap(Function.identity(), task -> TaskDecision.SUCCESS));
        }, dependencies);
    }

    @SafeVarargs
    public final CompositeTaskHandlerFactory<TRANSACTION, EXCEPTION, IDENTITY, SUPPLEMENT, CONTEXT> withResult(
        IDENTITY identity,
        BiFunction<Set<Task>, ? super SUPPLEMENT, Map<Task, TaskDecision>> step,
        IDENTITY... dependencies
    ) {
        return withAsync(
            identity,
            (tasks, supplement) -> CompletableFuture.completedStage(step.apply(tasks, supplement)),
            dependencies
        );
    }

    @SafeVarargs
    public final CompositeTaskHandlerFactory<TRANSACTION, EXCEPTION, IDENTITY, SUPPLEMENT, CONTEXT> withEachAsync(
        IDENTITY identity,
        BiFunction<Task, ? super SUPPLEMENT, CompletionStage<TaskDecision>> step,
        IDENTITY... dependencies
    ) {
        return withEachAsync(identity, Mode.PARALLEL, step, dependencies);
    }

    @SafeVarargs
    public final CompositeTaskHandlerFactory<TRANSACTION, EXCEPTION, IDENTITY, SUPPLEMENT, CONTEXT> withEachAsync(
        IDENTITY identity,
        Mode mode,
        BiFunction<Task, ? super SUPPLEMENT, CompletionStage<TaskDecision>> step,
        IDENTITY... dependencies
    ) {
        return withStep(identity, (tasks, supplement) -> mode.stream(tasks).map(task -> {
            try {
                return step.apply(task, supplement).handleAsync((result, throwable) -> {
                    TaskDecision decision;
                    if (throwable == null) {
                        decision = result;
                    } else if (throwable instanceof TaskResultException) {
                        decision = ((TaskResultException) throwable).toDecision();
                    } else {
                        decision = new TaskDecision(throwable);
                    }
                    return Collections.singletonMap(task, decision);
                }, executor);
            } catch (TaskResultException exception) {
                return CompletableFuture.completedStage(Collections.singletonMap(
                    task, new TaskDecision(exception.getResult(), exception)
                ));
            } catch (Throwable throwable) {
                return CompletableFuture.completedStage(Collections.singletonMap(
                    task, new TaskDecision(throwable)
                ));
            }
        }).reduce(
            CompletableFuture.completedStage(Collections.emptyMap()),
            (left, right) -> left.thenCombineAsync(right, TaskDecision::combine, executor)
        ), Arrays.asList(dependencies));
    }

    @SafeVarargs
    public final CompositeTaskHandlerFactory<TRANSACTION, EXCEPTION, IDENTITY, SUPPLEMENT, CONTEXT> withAsync(
        IDENTITY identity,
        BiFunction<Set<Task>, ? super SUPPLEMENT, CompletionStage<Map<Task, TaskDecision>>> step,
        IDENTITY... dependencies
    ) {
        return withStep(identity, (tasks, supplement) -> {
            try {
                return step.apply(tasks, supplement).handleAsync((decisions, throwable) -> {
                    if (throwable == null) {
                        return decisions;
                    } else if (throwable instanceof TaskResultException) {
                        return tasks.stream().collect(Collectors.toMap(
                            Function.identity(),
                            task -> ((TaskResultException) throwable).toDecision()
                        ));
                    } else {
                        return tasks.stream().collect(Collectors.toMap(
                            Function.identity(),
                            task -> new TaskDecision(throwable)
                        ));
                    }
                }, executor);
            } catch (TaskResultException exception) {
                return CompletableFuture.completedStage(tasks.stream().collect(Collectors.toMap(
                    Function.identity(),
                    task -> new TaskDecision(exception.getResult(), exception)
                )));
            } catch (Throwable throwable) {
                return CompletableFuture.completedStage(tasks.stream().collect(Collectors.toMap(
                    Function.identity(),
                    task -> new TaskDecision(throwable)
                )));
            }
        }, Arrays.asList(dependencies));
    }

    private CompositeTaskHandlerFactory<TRANSACTION, EXCEPTION, IDENTITY, SUPPLEMENT, CONTEXT> withStep(
        IDENTITY identity,
        BiFunction<Set<Task>, ? super SUPPLEMENT, CompletionStage<Map<Task, TaskDecision>>> step,
        List<IDENTITY> dependencies
    ) {
        if (!registrations.keySet().containsAll(dependencies)) {
            throw new IllegalArgumentException("Unknown dependencies: " + dependencies.stream()
                .filter(dependency -> !registrations.containsKey(dependency))
                .distinct()
                .collect(Collectors.toList()));
        }
        Map<IDENTITY, Registration<SUPPLEMENT, IDENTITY>> registrations = new LinkedHashMap<>(this.registrations);
        if (registrations.putIfAbsent(identity, new Registration<>(step, new HashSet<>(dependencies))) != null) {
            throw new IllegalArgumentException("Step already registered: " + identity);
        }
        return new CompositeTaskHandlerFactory<>(
            executor,
            failPendingOnClose,
            concurrency,
            contextFactory,
            supplementFactory,
            decoratorFactory,
            registrations
        );
    }

    @Override
    public TaskHandler<TRANSACTION, EXCEPTION> apply(String topic) {
        BiFunction<IDENTITY,
            BiFunction<Set<Task>, ? super SUPPLEMENT, CompletionStage<Map<Task, TaskDecision>>>,
            BiFunction<Set<Task>, ? super SUPPLEMENT, CompletionStage<Map<Task, TaskDecision>>>> decorator = decoratorFactory.apply(topic);
        BlockingQueue<Unit> queue = concurrency == 0 ? null : new ArrayBlockingQueue<>(concurrency);
        return new TaskHandler<>() {
            @Override
            public void accept(
                Set<Task> tasks,
                TaskCallback<TRANSACTION, EXCEPTION> callback,
                Consumer<Throwable> onFailure
            ) throws EXCEPTION {
                CONTEXT context = contextFactory.apply(topic, tasks);
                SUPPLEMENT supplement = supplementFactory.apply(context);
                CompletionStage<Map<Task, TaskDecision>> initial = CompletableFuture.completedStage(
                    tasks.stream().collect(Collectors.toMap(
                        Function.identity(),
                        task -> TaskDecision.SUCCESS
                    ))
                );
                Map<IDENTITY, CompletionStage<Map<Task, TaskDecision>>> dispatched = new HashMap<>();
                Set<IDENTITY> latest = new HashSet<>();
                while (!dispatched.keySet().containsAll(registrations.keySet())) {
                    registrations.entrySet().stream().filter(
                        entry -> !dispatched.containsKey(entry.getKey())
                    ).filter(
                        entry -> dispatched.keySet().containsAll(entry.getValue().getDependencies())
                    ).forEach(registration -> {
                        CompletionStage<Map<Task, TaskDecision>> future = initial;
                        for (IDENTITY dependency : registration.getValue().getDependencies()) {
                            future = future.thenCombineAsync(dispatched.get(dependency), TaskDecision::merge, executor);
                            latest.remove(dependency);
                        }
                        dispatched.put(registration.getKey(), future.thenComposeAsync(current -> {
                            Set<Task> active = current.entrySet().stream()
                                .filter(entry -> entry.getValue().isContinued())
                                .map(Map.Entry::getKey)
                                .collect(Collectors.toCollection(LinkedHashSet::new));
                            if (active.isEmpty()) {
                                return CompletableFuture.completedStage(current);
                            } else {
                                return decorator.apply(registration.getKey(), registration.getValue().getStep())
                                    .apply(Collections.unmodifiableSet(active), supplement)
                                    .thenApplyAsync(next -> {
                                        if (!current.keySet().containsAll(next.keySet())) {
                                            throw new IllegalStateException(
                                                "Received decisions for unknown tasks in step " + registration.getKey() + ": " + next
                                                    .keySet()
                                                    .stream()
                                                    .filter(task -> !current.containsKey(task))
                                                    .sorted()
                                                    .collect(Collectors.toList())
                                            );
                                        }
                                        return current.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                                            if (active.contains(entry.getKey())) {
                                                TaskDecision decision = next.get(entry.getKey());
                                                return entry.getValue().merge(decision == null ? new TaskDecision(
                                                    TaskResult.FAILURE,
                                                    "Missing task decision in step " + registration.getKey()
                                                ) : decision);
                                            } else {
                                                return entry.getValue();
                                            }
                                        }));
                                    }, executor);
                            }
                        }, executor));
                        latest.add(registration.getKey());
                    });
                }
                CompletionStage<?> result = latest.stream().map(dispatched::get).reduce(initial, (left, right) ->
                    left.thenCombineAsync(right, TaskDecision::merge, executor)
                ).thenComposeAsync(decisions -> {
                    try {
                        return context.apply(
                            Collections.unmodifiableMap(decisions), executor, supplement
                        ).thenApplyAsync(completion -> (TaskCompletion<TRANSACTION, EXCEPTION>) transaction -> Stream.concat(
                            decisions.entrySet().stream(),
                            completion.complete(transaction).entrySet().stream()
                        ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, TaskDecision::merge)), executor);
                    } catch (Throwable throwable) {
                        return CompletableFuture.failedStage(throwable);
                    }
                }, executor).thenComposeAsync(completion -> {
                    try {
                        callback.accept(completion);
                        context.onAfterTransaction(executor, supplement);
                        return CompletableFuture.completedStage(null);
                    } catch (Throwable throwable) {
                        return CompletableFuture.failedStage(throwable);
                    }
                }, executor).exceptionally(throwable -> {
                    try {
                        onFailure.accept(throwable);
                    } catch (Throwable ignored) { }
                    return null;
                });
                Unit unit = new Unit(onFailure, new CountDownLatch(1));
                try {
                    if (queue == null) {
                        try {
                            result.toCompletableFuture().get();
                        } catch (ExecutionException ignored) { }
                    } else {
                        queue.put(unit);
                    }
                } catch (InterruptedException exception) {
                    if (failPendingOnClose) {
                        onFailure.accept(exception);
                        return;
                    } else if (queue != null) {
                        do {
                            try {
                                queue.put(unit);
                                break;
                            } catch (Throwable ignored) { }
                        } while (true);
                    }
                }
                if (queue != null) {
                    result.thenRunAsync(() -> {
                        try {
                            unit.getLatch().countDown();
                        } finally {
                            queue.remove(unit);
                        }
                    }, executor);
                }
            }

            @Override
            public void close() {
                if (queue != null) {
                    if (failPendingOnClose) {
                        Unit unit;
                        while ((unit = queue.poll()) != null) {
                            unit.getOnFailure().accept(new TaskHandlerShutdownException());
                        }
                    } else {
                        while (!queue.isEmpty()) {
                            queue.forEach(unit -> {
                                try {
                                    unit.getLatch().await();
                                } catch (Throwable ignored) { }
                            });
                        }
                    }
                }
            }
        };
    }

    public enum Mode {

        SEQUENTIAL(Collection::stream),
        PARALLEL(Collection::parallelStream);

        private final Function<Collection<Task>, Stream<Task>> streamer;

        Mode(Function<Collection<Task>, Stream<Task>> streamer) {
            this.streamer = streamer;
        }

        Stream<Task> stream(Collection<Task> tasks) {
            return streamer.apply(tasks);
        }
    }

    static class Registration<SUPPLEMENT, IDENTITY> {

        private final BiFunction<Set<Task>, ? super SUPPLEMENT, CompletionStage<Map<Task, TaskDecision>>> step;
        private final Set<IDENTITY> dependencies;

        Registration(
            BiFunction<Set<Task>, ? super SUPPLEMENT, CompletionStage<Map<Task, TaskDecision>>> step,
            Set<IDENTITY> dependencies
        ) {
            this.step = step;
            this.dependencies = dependencies;
        }

        BiFunction<Set<Task>, ? super SUPPLEMENT, CompletionStage<Map<Task, TaskDecision>>> getStep() {
            return step;
        }

        Set<IDENTITY> getDependencies() {
            return dependencies;
        }
    }

    static class Unit {

        private final Consumer<Throwable> onFailure;
        private final CountDownLatch latch;

        Unit(Consumer<Throwable> onFailure, CountDownLatch latch) {
            this.onFailure = onFailure;
            this.latch = latch;
        }

        Consumer<Throwable> getOnFailure() {
            return onFailure;
        }

        CountDownLatch getLatch() {
            return latch;
        }
    }
}
