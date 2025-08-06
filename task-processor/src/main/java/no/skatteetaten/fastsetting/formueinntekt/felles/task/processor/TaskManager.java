package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.Task;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskDecision;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskResult;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskSource;

public class TaskManager<TRANSACTION, EXCEPTION extends Exception, TRACE> implements TaskProcessor {

    private final String topic;

    private final Executor executor;

    private final TaskSource<TRANSACTION, EXCEPTION> source;

    private final TaskLimiter limiter;

    private final TaskDispatcher<TRANSACTION, EXCEPTION> dispatcher;

    private final Function<String, ? extends TaskHandler<TRANSACTION, EXCEPTION>> handlerFactory;

    private final TaskListener<TRACE> listener;

    private final TaskSource.Order order;
    private final TaskSource.Condition condition;

    private final int size;

    private final long pause;
    private final TimeUnit timeUnit;

    private final AtomicBoolean alive = new AtomicBoolean();
    private final AtomicReference<Thread> current = new AtomicReference<>();
    private volatile CountDownLatch completionLatch = new CountDownLatch(0);

    private volatile Runnable continuation = () -> { };

    public TaskManager(
        String topic,
        Executor executor,
        TaskSource<TRANSACTION, EXCEPTION> source,
        TaskDispatcher<TRANSACTION, EXCEPTION> dispatcher,
        TaskLimiter limiter,
        Function<String, ? extends TaskHandler<TRANSACTION, EXCEPTION>> handlerFactory,
        TaskListener<TRACE> listener,
        TaskSource.Order order,
        TaskSource.Condition condition,
        int size,
        long pause,
        TimeUnit timeUnit
    ) {
        this.topic = topic;
        this.executor = executor;
        this.source = source;
        this.limiter = limiter;
        this.dispatcher = dispatcher;
        this.handlerFactory = handlerFactory;
        this.listener = listener;
        this.order = order;
        this.condition = condition;
        this.size = size;
        this.pause = pause;
        this.timeUnit = timeUnit;
    }

    @Override
    public synchronized boolean start(long timeout, TimeUnit timeUnit) throws InterruptedException, TimeoutException {
        if (alive.get()) {
            return false;
        }
        CountDownLatch startLatch = new CountDownLatch(1);
        completionLatch = new CountDownLatch(1);
        executor.execute(() -> {
            if (alive.compareAndSet(false, true)) {
                current.set(Thread.currentThread());
                startLatch.countDown();
                try (TaskHandler<TRANSACTION, EXCEPTION> handler = handlerFactory.apply(topic)) {
                    CyclicBarrier barrier = new CyclicBarrier(2);
                    continuation = barrier::reset;
                    while (current.get() != null && !Thread.interrupted()) {
                        TaskLimiter.Token token = limiter.request(topic);
                        try {
                            Set<Task> tasks;
                            try {
                                tasks = dispatcher.apply(transaction -> source.poll(transaction, topic, order, size));
                            } catch (Throwable throwable) {
                                listener.onFatal(topic, throwable);
                                token.release();
                                continue;
                            }
                            if (tasks.isEmpty()) {
                                listener.onEmpty(topic);
                                token.release();
                                try {
                                    barrier.await(pause, this.timeUnit);
                                    throw new IllegalStateException();
                                } catch (InterruptedException ignored) {
                                    Thread.currentThread().interrupt();
                                    break;
                                } catch (BrokenBarrierException ignored) {
                                    continue;
                                } catch (TimeoutException ignored) {
                                    barrier.reset();
                                    continue;
                                }
                            }
                            TRACE trace = listener.onStart(topic, Collections.unmodifiableSet(tasks));
                            try {
                                handler.accept(Collections.unmodifiableSet(tasks), callback -> {
                                    try {
                                        listener.onCallback(topic, trace, Collections.unmodifiableSet(tasks));
                                        Map<Task, TaskDecision> committed = dispatcher.apply(transaction -> {
                                            Map<Task, TaskDecision> decisions = new HashMap<>(callback.complete(transaction));
                                            if (!tasks.containsAll(decisions.keySet())) {
                                                throw new IllegalStateException(
                                                    "Received decisions for unknown tasks: " + decisions
                                                        .keySet()
                                                        .stream()
                                                        .filter(task -> !tasks.contains(task))
                                                        .sorted()
                                                        .collect(Collectors.toList())
                                                );
                                            } else if (!decisions.keySet().containsAll(tasks)) {
                                                tasks.stream().filter(task -> !decisions.containsKey(task)).forEach(task -> decisions.put(task, new TaskDecision(
                                                    TaskResult.FAILURE,
                                                    "Task was not included in task processor's result set"
                                                )));
                                            }
                                            source.complete(transaction, topic, decisions);
                                            return decisions;
                                        });
                                        try {
                                            try {
                                                token.release();
                                            } finally {
                                                listener.onComplete(topic, trace, Collections.unmodifiableMap(committed));
                                            }
                                        } catch (Throwable ignored) { }
                                    } catch (Throwable throwable) {
                                        doFailAll(trace, tasks, throwable, token);
                                    }
                                }, throwable -> doFailAll(trace, tasks, throwable, token));
                            } catch (Throwable throwable) {
                                doFailAll(trace, tasks, throwable, token);
                            } finally {
                                listener.onDispatched(topic, trace, Collections.unmodifiableSet(tasks));
                            }
                        } catch (Throwable throwable) {
                            try {
                                token.release();
                            } catch (Throwable suppressed) {
                                throwable.addSuppressed(suppressed);
                            }
                            throw throwable;
                        }
                    }
                } catch (Throwable throwable) {
                    listener.onFatal(topic, throwable);
                } finally {
                    current.set(null);
                    alive.set(false);
                    completionLatch.countDown();
                }
            } else {
                throw new IllegalStateException("Process for " + topic + " is already running");
            }
        });
        try {
            if (!startLatch.await(timeout, timeUnit)) {
                throw new TimeoutException();
            }
        } catch (Throwable t) {
            Thread thread = current.getAndSet(null);
            thread.interrupt();
            throw t;
        }
        return true;
    }

    @Override
    public synchronized boolean stop(long timeout, TimeUnit timeUnit) throws InterruptedException, TimeoutException {
        if (!alive.get()) {
            return false;
        }
        Thread thread = current.getAndSet(null);
        if (thread != null) {
            thread.interrupt();
        }
        continuation.run();
        continuation = () -> { };
        if (!completionLatch.await(timeout, timeUnit)) {
            throw new TimeoutException();
        }
        return true;
    }

    @Override
    public boolean resume() {
        if (!alive.get()) {
            return false;
        }
        continuation.run();
        return true;
    }

    @Override
    public boolean isActive() {
        return alive.get();
    }

    @Override
    public String getTopic() {
        return topic;
    }

    private void doFailAll(TRACE trace, Set<Task> tasks, Throwable throwable, TaskLimiter.Token token) {
        try {
            dispatcher.accept(transaction -> source.complete(transaction, topic, tasks.stream().collect(Collectors.toMap(
                Function.identity(), task -> new TaskDecision(throwable)
            ))));
        } catch (Throwable fatal) {
            fatal.addSuppressed(throwable);
            listener.onFatal(topic, fatal);
        } finally {
            try {
                try {
                    token.release();
                } finally {
                    listener.onError(topic, trace, Collections.unmodifiableSet(tasks), throwable);
                }
            } catch (Throwable ignored) { }
        }
    }
}
