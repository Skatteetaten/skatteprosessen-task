package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.jdbc;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.TaskProcessor;

public class DefaultTaskChangeHandler implements BiConsumer<TaskProcessor, TaskChangeEvent> {

    private final long timeout;

    private final TimeUnit timeUnit;

    private final Consumer<Throwable> onError;

    public DefaultTaskChangeHandler(long timeout, TimeUnit timeUnit) {
        this.timeout = timeout;
        this.timeUnit = timeUnit;
        onError = t -> { };
    }

    public DefaultTaskChangeHandler(long timeout, TimeUnit timeUnit, Consumer<Throwable> onError) {
        this.timeout = timeout;
        this.timeUnit = timeUnit;
        this.onError = onError;
    }

    @Override
    public void accept(TaskProcessor processor, TaskChangeEvent event) {
        try {
            switch (event) {
            case ACTIVATION:
                processor.findActivation().ifPresent(activation -> {
                    try {
                        switch (activation) {
                        case ACTIVE:
                            if (!processor.isActive()) {
                                processor.start(timeout, timeUnit);
                            }
                        case INACTIVE:
                            if (processor.isActive()) {
                                processor.stop(timeout, timeUnit);
                            }
                        default:
                            throw new IllegalStateException("Unknown activation: " + activation);
                        }
                    } catch (InterruptedException | TimeoutException e) {
                        throw new IllegalStateException(e);
                    }
                });
                break;
            case WORK:
                processor.resume();
                break;
            }
        } catch (Exception e) {
            onError.accept(e);
        }
    }
}
