package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public interface TaskProcessor {

    boolean start(long timeout, TimeUnit timeUnit) throws InterruptedException, TimeoutException;

    boolean stop(long timeout, TimeUnit timeUnit) throws InterruptedException, TimeoutException;

    boolean resume();

    boolean isActive();

    String getTopic();
}
