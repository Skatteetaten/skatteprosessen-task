package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public interface TaskProcessor {

    boolean start(long timeout, TimeUnit timeUnit) throws InterruptedException, TimeoutException;

    boolean stop(long timeout, TimeUnit timeUnit) throws InterruptedException, TimeoutException;

    boolean resume();

    boolean isActive();

    String getTopic();

    default Optional<Activation> findActivation() {
        return Optional.empty();
    }

    default boolean initialize(long timeout, TimeUnit timeUnit) throws InterruptedException, TimeoutException {
        start(timeout, timeUnit);
        return true;
    }

    default boolean shutdown(long timeout, TimeUnit timeUnit) throws InterruptedException, TimeoutException {
        stop(timeout, timeUnit);
        return true;
    }

    enum Activation {
        ACTIVE, INACTIVE
    }
}
