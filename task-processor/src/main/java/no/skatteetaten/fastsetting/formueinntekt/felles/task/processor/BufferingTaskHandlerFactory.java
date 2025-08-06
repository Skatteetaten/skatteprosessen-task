package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.Task;

public class BufferingTaskHandlerFactory<TRANSACTION, EXCEPTION extends Exception, BAGGAGE> implements Function<String, TaskHandler<TRANSACTION, EXCEPTION>> {

    private final Executor executor;

    private final boolean failPendingOnClose;

    private final int workers;

    private final int buffer;

    private final long poll;
    private final TimeUnit timeUnit;

    private final Function<String, ? extends TaskHandler<TRANSACTION, EXCEPTION>> delegate;

    private final Supplier<BAGGAGE> baggageSupplier;

    private final Consumer<BAGGAGE> baggageConsumer;

    private final BiConsumer<String, Collection<?>> observer;

    private BufferingTaskHandlerFactory(
        Executor executor,
        boolean failPendingOnClose,
        int workers,
        int buffer,
        long poll,
        TimeUnit timeUnit,
        Function<String, ? extends TaskHandler<TRANSACTION, EXCEPTION>> delegate,
        Supplier<BAGGAGE> baggageSupplier,
        Consumer<BAGGAGE> baggageConsumer,
        BiConsumer<String, Collection<?>> observer
    ) {
        this.executor = executor;
        this.failPendingOnClose = failPendingOnClose;
        this.workers = workers;
        this.buffer = buffer;
        this.poll = poll;
        this.timeUnit = timeUnit;
        this.delegate = delegate;
        this.baggageConsumer = baggageConsumer;
        this.baggageSupplier = baggageSupplier;
        this.observer = observer;
    }

    public static <TRANSACTION, EXCEPTION extends Exception, BAGGAGE> BufferingTaskHandlerFactory<TRANSACTION, EXCEPTION, BAGGAGE> withBaggage(
        Executor executor,
        boolean failPendingOnClose,
        int buffer,
        int workers,
        long poll,
        TimeUnit timeUnit,
        Function<String, ? extends TaskHandler<TRANSACTION, EXCEPTION>> delegate,
        Supplier<BAGGAGE> baggageSupplier,
        Consumer<BAGGAGE> baggageConsumer
    ) {
        return new BufferingTaskHandlerFactory<>(
            executor,
            failPendingOnClose,
            workers,
            buffer,
            poll,
            timeUnit,
            delegate,
            baggageSupplier,
            baggageConsumer,
            (topic, queue) -> { }
        );
    }

    public static <TRANSACTION, EXCEPTION extends Exception> BufferingTaskHandlerFactory<TRANSACTION, EXCEPTION, Void> withoutBaggage(
        Executor executor,
        boolean failPendingOnClose,
        int buffer,
        int workers,
        long poll,
        TimeUnit timeUnit,
        Function<String, ? extends TaskHandler<TRANSACTION, EXCEPTION>> delegate
    ) {
        return new BufferingTaskHandlerFactory<>(
            executor,
            failPendingOnClose,
            workers,
            buffer,
            poll,
            timeUnit,
            delegate,
            () -> null,
            ignored -> { },
            (topic, queue) -> { }
        );
    }

    public BufferingTaskHandlerFactory<TRANSACTION, EXCEPTION, BAGGAGE> withObserver(BiConsumer<String, Collection<?>> observer) {
        return new BufferingTaskHandlerFactory<>(
            executor,
            failPendingOnClose,
            workers,
            buffer,
            poll,
            timeUnit,
            delegate,
            baggageSupplier,
            baggageConsumer,
            this.observer.andThen(observer)
        );
    }

    @Override
    public TaskHandler<TRANSACTION, EXCEPTION> apply(String topic) {
        BlockingQueue<Unit<TRANSACTION, EXCEPTION, BAGGAGE>> queue = new ArrayBlockingQueue<>(buffer);
        observer.accept(topic, Collections.unmodifiableCollection(queue));
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
                            Unit<TRANSACTION, EXCEPTION, BAGGAGE> unit = queue.poll(poll, timeUnit);
                            if (unit == null) {
                                continue;
                            }
                            try {
                                baggageConsumer.accept(unit.getBaggage());
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
                    BufferingTaskHandlerFactory.Unit<TRANSACTION, EXCEPTION, BAGGAGE> unit;
                    while ((unit = queue.poll()) != null) {
                        Thread.currentThread().interrupt();
                        try {
                            baggageConsumer.accept(unit.getBaggage());
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
                Unit<TRANSACTION, EXCEPTION, BAGGAGE> unit = new Unit<>(tasks, callback, onFailure, baggageSupplier.get());
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

    static class Unit<TRANSACTION, EXCEPTION extends Exception, BAGGAGE> {

        private final Set<Task> tasks;
        private final TaskCallback<TRANSACTION, EXCEPTION> callback;
        private final Consumer<Throwable> onFailure;
        private final BAGGAGE baggage;

        Unit(Set<Task> tasks, TaskCallback<TRANSACTION, EXCEPTION> callback, Consumer<Throwable> onFailure, BAGGAGE baggage) {
            this.tasks = tasks;
            this.callback = callback;
            this.onFailure = onFailure;
            this.baggage = baggage;
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

        BAGGAGE getBaggage() {
            return baggage;
        }
    }
}
