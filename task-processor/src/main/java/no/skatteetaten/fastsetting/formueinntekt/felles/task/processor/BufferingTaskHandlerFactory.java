package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor;

import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.Task;

public class BufferingTaskHandlerFactory<TRANSACTION, EXCEPTION extends Exception> implements Function<String, TaskHandler<TRANSACTION, EXCEPTION>> {

    private final Executor executor;

    private final boolean failPendingOnClose;

    private final int buffer, workers;

    private final long poll;
    private final TimeUnit timeUnit;

    private final Function<String, ? extends TaskHandler<TRANSACTION, EXCEPTION>> delegate;

    public BufferingTaskHandlerFactory(
        Executor executor,
        boolean failPendingOnClose,
        int buffer,
        int workers,
        long poll,
        TimeUnit timeUnit,
        Function<String, ? extends TaskHandler<TRANSACTION, EXCEPTION>> delegate
    ) {
        this.executor = executor;
        this.failPendingOnClose = failPendingOnClose;
        this.buffer = buffer;
        this.workers = workers;
        this.poll = poll;
        this.timeUnit = timeUnit;
        this.delegate = delegate;
    }

    @Override
    public TaskHandler<TRANSACTION, EXCEPTION> apply(String topic) {
        BlockingQueue<Unit<TRANSACTION, EXCEPTION>> queue = new ArrayBlockingQueue<>(buffer);
        CountDownLatch latch = new CountDownLatch(workers);
        Set<Thread> threads = ConcurrentHashMap.newKeySet();
        AtomicBoolean closed = new AtomicBoolean();
        for (int index = 0; index < workers; index++) {
            TaskHandler<TRANSACTION, EXCEPTION> handler = delegate.apply(topic);
            executor.execute(() -> {
                threads.add(Thread.currentThread());
                try {
                    while (!closed.get() && !Thread.interrupted()) {
                        try {
                            Unit<TRANSACTION, EXCEPTION> unit = queue.poll(poll, timeUnit);
                            if (unit == null) {
                                continue;
                            }
                            try {
                                handler.accept(unit.getTasks(), unit.getCallback(), unit.getOnFailure());
                            } catch (Throwable throwable) {
                                try {
                                    unit.getOnFailure().accept(throwable);
                                } catch (Throwable ignored) { }
                                if (throwable instanceof InterruptedException) {
                                    throw (InterruptedException) throwable;
                                }
                            }
                        } catch (InterruptedException ignored) {
                            break;
                        }
                    }
                    BufferingTaskHandlerFactory.Unit<TRANSACTION, EXCEPTION> unit;
                    while ((unit = queue.poll()) != null) {
                        Thread.currentThread().interrupt();
                        try {
                            if (failPendingOnClose) {
                                unit.getOnFailure().accept(new TaskHandlerShutdownException());
                            } else {
                                try {
                                    handler.accept(unit.getTasks(), unit.getCallback(), unit.getOnFailure());
                                } catch (Throwable throwable) {
                                    unit.getOnFailure().accept(throwable);
                                }
                            }
                        } catch (Throwable ignored) { }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }
        return new TaskHandler<>() {
            @Override
            public void accept(
                Set<Task> tasks,
                TaskCallback<TRANSACTION, EXCEPTION> callback,
                Consumer<Throwable> onFailure
            ) {
                Unit<TRANSACTION, EXCEPTION> unit = new Unit<>(tasks, callback, onFailure);
                try {
                    queue.put(unit);
                    return;
                } catch (InterruptedException ignored) { }
                if (failPendingOnClose) {
                    try {
                        unit.getOnFailure().accept(new TaskHandlerShutdownException());
                    } catch (Throwable ignored) { }
                } else {
                    do {
                        try {
                            queue.put(unit);
                            break;
                        } catch (Throwable ignored) { }
                    } while (true);
                }
            }

            @Override
            public void close() {
                closed.set(true);
                threads.forEach(Thread::interrupt);
                do {
                    try {
                        latch.await();
                        break;
                    } catch (InterruptedException ignored) { }
                } while (true);
            }
        };
    }

    static class Unit<TRANSACTION, EXCEPTION extends Exception> {

        private final Set<Task> tasks;
        private final TaskCallback<TRANSACTION, EXCEPTION> callback;
        private final Consumer<Throwable> onFailure;

        Unit(Set<Task> tasks, TaskCallback<TRANSACTION, EXCEPTION> callback, Consumer<Throwable> onFailure) {
            this.tasks = tasks;
            this.callback = callback;
            this.onFailure = onFailure;
        }

        Set<Task> getTasks() {
            return tasks;
        }

        TaskCallback<TRANSACTION, EXCEPTION> getCallback() {
            return callback;
        }

        Consumer<Throwable> getOnFailure() {
            return onFailure;
        }
    }
}
