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
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskResultException;

public class CompositeTaskHandlerFactory<TRANSACTION,
    EXCEPTION extends Exception,
    IDENTITY,
    CONTEXT extends TaskContext<TRANSACTION, EXCEPTION>> implements Function<String, TaskHandler<TRANSACTION, EXCEPTION>> {

    private final Executor executor;

    private final boolean failPendingOnClose;
    private final int concurrency;

    private final TaskContextFactory<TRANSACTION, EXCEPTION, CONTEXT> contextFactory;
    private final Function<String, BiFunction<IDENTITY,
        BiFunction<Set<Task>, ? super CONTEXT, CompletionStage<Map<Task, TaskDecision>>>,
        BiFunction<Set<Task>, ? super CONTEXT, CompletionStage<Map<Task, TaskDecision>>>>> decoratorFactory;

    private final Map<IDENTITY, Registration<CONTEXT, IDENTITY>> registrations;

    public CompositeTaskHandlerFactory(
        Executor executor,
        boolean failPendingOnClose,
        int concurrency,
        TaskContextFactory<TRANSACTION, EXCEPTION, CONTEXT> contextFactory
    ) {
        this.executor = executor;
        this.failPendingOnClose = failPendingOnClose;
        this.concurrency = concurrency;
        this.contextFactory = contextFactory;
        decoratorFactory = topic -> (identity, step) -> step;
        registrations = Collections.emptyMap();
    }

    public CompositeTaskHandlerFactory(
        Executor executor,
        boolean failPendingOnClose,
        int concurrency,
        TaskContextFactory<TRANSACTION, EXCEPTION, CONTEXT> contextFactory,
        Function<String, BiFunction<IDENTITY,
            BiFunction<Set<Task>, ? super CONTEXT, CompletionStage<Map<Task, TaskDecision>>>,
            BiFunction<Set<Task>, ? super CONTEXT, CompletionStage<Map<Task, TaskDecision>>>>> decoratorFactory
    ) {
        this.executor = executor;
        this.failPendingOnClose = failPendingOnClose;
        this.concurrency = concurrency;
        this.contextFactory = contextFactory;
        this.decoratorFactory = decoratorFactory;
        registrations = Collections.emptyMap();
    }

    private CompositeTaskHandlerFactory(
        Executor executor,
        boolean failPendingOnClose,
        int concurrency,
        TaskContextFactory<TRANSACTION, EXCEPTION, CONTEXT> contextFactory,
        Function<String, BiFunction<IDENTITY,
            BiFunction<Set<Task>, ? super CONTEXT, CompletionStage<Map<Task, TaskDecision>>>,
            BiFunction<Set<Task>, ? super CONTEXT, CompletionStage<Map<Task, TaskDecision>>>>> decoratorFactory,
        Map<IDENTITY, Registration<CONTEXT, IDENTITY>> registrations
    ) {
        this.executor = executor;
        this.failPendingOnClose = failPendingOnClose;
        this.concurrency = concurrency;
        this.contextFactory = contextFactory;
        this.decoratorFactory = decoratorFactory;
        this.registrations = registrations;
    }

    @SafeVarargs
    public final CompositeTaskHandlerFactory<TRANSACTION, EXCEPTION, IDENTITY, CONTEXT> withEach(
        IDENTITY identity,
        BiConsumer<Task, ? super CONTEXT> step,
        IDENTITY... dependencies
    ) {
        return withEachNoResult(identity, step, dependencies);
    }

    @SafeVarargs
    public final CompositeTaskHandlerFactory<TRANSACTION, EXCEPTION, IDENTITY, CONTEXT> withEach(
        IDENTITY identity,
        Mode mode,
        BiConsumer<Task, ? super CONTEXT> step,
        IDENTITY... dependencies
    ) {
        return withEachNoResult(identity, mode, step, dependencies);
    }

    @SafeVarargs
    public final CompositeTaskHandlerFactory<TRANSACTION, EXCEPTION, IDENTITY, CONTEXT> withEach(
        IDENTITY identity,
        BiFunction<Task, ? super CONTEXT, TaskDecision> step,
        IDENTITY... dependencies
    ) {
        return withEachResult(identity, step, dependencies);
    }

    @SafeVarargs
    public final CompositeTaskHandlerFactory<TRANSACTION, EXCEPTION, IDENTITY, CONTEXT> withEach(
        IDENTITY identity,
        Mode mode,
        BiFunction<Task, ? super CONTEXT, TaskDecision> step,
        IDENTITY... dependencies
    ) {
        return withEachResult(identity, mode, step, dependencies);
    }

    @SafeVarargs
    public final CompositeTaskHandlerFactory<TRANSACTION, EXCEPTION, IDENTITY, CONTEXT> with(
        IDENTITY identity,
        BiConsumer<Set<Task>, ? super CONTEXT> step,
        IDENTITY... dependencies
    ) {
        return withNoResult(identity, step, dependencies);
    }

    @SafeVarargs
    public final CompositeTaskHandlerFactory<TRANSACTION, EXCEPTION, IDENTITY, CONTEXT> with(
        IDENTITY identity,
        BiFunction<Set<Task>, ? super CONTEXT, Map<Task, TaskDecision>> step,
        IDENTITY... dependencies
    ) {
        return withResult(identity, step, dependencies);
    }

    @SafeVarargs
    public final CompositeTaskHandlerFactory<TRANSACTION, EXCEPTION, IDENTITY, CONTEXT> withEachNoResult(
        IDENTITY identity,
        BiConsumer<Task, ? super CONTEXT> step,
        IDENTITY... dependencies
    ) {
        return withEachNoResult(identity, Mode.PARALLEL, step, dependencies);
    }

    @SafeVarargs
    public final CompositeTaskHandlerFactory<TRANSACTION, EXCEPTION, IDENTITY, CONTEXT> withEachNoResult(
        IDENTITY identity,
        Mode mode,
        BiConsumer<Task, ? super CONTEXT> step,
        IDENTITY... dependencies
    ) {
        return withEachResult(identity, mode, (task, context) -> {
            step.accept(task, context);
            return TaskDecision.SUCCESS;
        }, dependencies);
    }

    @SafeVarargs
    public final CompositeTaskHandlerFactory<TRANSACTION, EXCEPTION, IDENTITY, CONTEXT> withEachResult(
        IDENTITY identity,
        BiFunction<Task, ? super CONTEXT, TaskDecision> step,
        IDENTITY... dependencies
    ) {
        return withEachResult(identity, Mode.PARALLEL, step, dependencies);
    }

    @SafeVarargs
    public final CompositeTaskHandlerFactory<TRANSACTION, EXCEPTION, IDENTITY, CONTEXT> withEachResult(
        IDENTITY identity,
        Mode mode,
        BiFunction<Task, ? super CONTEXT, TaskDecision> step,
        IDENTITY... dependencies
    ) {
        return withEachAsync(
            identity,
            mode,
            (task, context) -> CompletableFuture.completedStage(step.apply(task, context)),
            dependencies
        );
    }

    @SafeVarargs
    public final CompositeTaskHandlerFactory<TRANSACTION, EXCEPTION, IDENTITY, CONTEXT> withNoResult(
        IDENTITY identity,
        BiConsumer<Set<Task>, ? super CONTEXT> step,
        IDENTITY... dependencies
    ) {
        return withResult(identity, (tasks, context) -> {
            step.accept(tasks, context);
            return tasks.stream().collect(Collectors.toMap(Function.identity(), task -> TaskDecision.SUCCESS));
        }, dependencies);
    }

    @SafeVarargs
    public final CompositeTaskHandlerFactory<TRANSACTION, EXCEPTION, IDENTITY, CONTEXT> withResult(
        IDENTITY identity,
        BiFunction<Set<Task>, ? super CONTEXT, Map<Task, TaskDecision>> step,
        IDENTITY... dependencies
    ) {
        return withAsync(
            identity,
            (tasks, context) -> CompletableFuture.completedStage(step.apply(tasks, context)),
            dependencies
        );
    }

    @SafeVarargs
    public final CompositeTaskHandlerFactory<TRANSACTION, EXCEPTION, IDENTITY, CONTEXT> withEachAsync(
        IDENTITY identity,
        BiFunction<Task, ? super CONTEXT, CompletionStage<TaskDecision>> step,
        IDENTITY... dependencies
    ) {
        return withEachAsync(identity, Mode.PARALLEL, step, dependencies);
    }

    @SafeVarargs
    public final CompositeTaskHandlerFactory<TRANSACTION, EXCEPTION, IDENTITY, CONTEXT> withEachAsync(
        IDENTITY identity,
        Mode mode,
        BiFunction<Task, ? super CONTEXT, CompletionStage<TaskDecision>> step,
        IDENTITY... dependencies
    ) {
        return withStep(identity, (tasks, context) -> mode.stream(tasks).map(task -> {
            try {
                return step.apply(task, context).handleAsync((result, throwable) -> {
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
    public final CompositeTaskHandlerFactory<TRANSACTION, EXCEPTION, IDENTITY, CONTEXT> withAsync(
        IDENTITY identity,
        BiFunction<Set<Task>, ? super CONTEXT, CompletionStage<Map<Task, TaskDecision>>> step,
        IDENTITY... dependencies
    ) {
        return withStep(identity, (tasks, context) -> {
            try {
                return step.apply(tasks, context).handleAsync((decisions, throwable) -> {
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

    private CompositeTaskHandlerFactory<TRANSACTION, EXCEPTION, IDENTITY, CONTEXT> withStep(
        IDENTITY identity,
        BiFunction<Set<Task>, ? super CONTEXT, CompletionStage<Map<Task, TaskDecision>>> step,
        List<IDENTITY> dependencies
    ) {
        if (!registrations.keySet().containsAll(dependencies)) {
            throw new IllegalArgumentException("Unknown dependencies: " + dependencies.stream()
                .filter(dependency -> !registrations.containsKey(dependency))
                .distinct()
                .collect(Collectors.toList()));
        }
        Map<IDENTITY, Registration<CONTEXT, IDENTITY>> registrations = new LinkedHashMap<>(this.registrations);
        if (registrations.putIfAbsent(identity, new Registration<>(step, new HashSet<>(dependencies))) != null) {
            throw new IllegalArgumentException("Step already registered: " + identity);
        }
        return new CompositeTaskHandlerFactory<>(
            executor,
            failPendingOnClose,
            concurrency,
            contextFactory,
            decoratorFactory,
            registrations
        );
    }

    @Override
    public TaskHandler<TRANSACTION, EXCEPTION> apply(String topic) {
        BiFunction<IDENTITY,
            BiFunction<Set<Task>, ? super CONTEXT, CompletionStage<Map<Task, TaskDecision>>>,
            BiFunction<Set<Task>, ? super CONTEXT, CompletionStage<Map<Task, TaskDecision>>>> decorator = decoratorFactory.apply(topic);
        BlockingQueue<Unit> queue = concurrency == 0 ? null : new ArrayBlockingQueue<>(concurrency);
        return new TaskHandler<>() {
            @Override
            public void accept(
                Set<Task> tasks,
                TaskCallback<TRANSACTION, EXCEPTION> callback,
                Consumer<Throwable> onFailure
            ) throws EXCEPTION {
                CONTEXT context = contextFactory.apply(topic, tasks);
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
                        dispatched.put(registration.getKey(), future.thenComposeAsync(decisions -> {
                            Set<Task> active = decisions.entrySet().stream()
                                .filter(entry -> entry.getValue().isContinued())
                                .map(Map.Entry::getKey)
                                .collect(Collectors.toCollection(LinkedHashSet::new));
                            if (active.isEmpty()) {
                                return CompletableFuture.completedStage(decisions);
                            } else {
                                return decorator.apply(registration.getKey(), registration.getValue().getStep())
                                    .apply(Collections.unmodifiableSet(active), context)
                                    .thenApplyAsync(additional -> Stream.concat(
                                        decisions.entrySet().stream().filter(entry -> !active.contains(entry.getKey())),
                                        additional.entrySet().stream()
                                    ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)), executor);
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
                            Collections.unmodifiableMap(decisions), executor
                        ).thenApplyAsync(completion -> (TaskContextCompletion<TRANSACTION, EXCEPTION>) transaction -> Stream.concat(
                            decisions.entrySet().stream(),
                            completion.complete(transaction).entrySet().stream()
                        ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, TaskDecision::merge)), executor);
                    } catch (Throwable throwable) {
                        return CompletableFuture.failedStage(throwable);
                    }
                }, executor).thenComposeAsync(completion -> {
                    try {
                        callback.accept(completion.toCompletion());
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

    static class Registration<CONTEXT, IDENTITY> {

        private final BiFunction<Set<Task>, ? super CONTEXT, CompletionStage<Map<Task, TaskDecision>>> step;
        private final Set<IDENTITY> dependencies;

        Registration(
            BiFunction<Set<Task>, ? super CONTEXT, CompletionStage<Map<Task, TaskDecision>>> step,
            Set<IDENTITY> dependencies
        ) {
            this.step = step;
            this.dependencies = dependencies;
        }

        BiFunction<Set<Task>, ? super CONTEXT, CompletionStage<Map<Task, TaskDecision>>> getStep() {
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
