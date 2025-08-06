package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

@FunctionalInterface
public interface TaskLimiter {

    Token request(String topic) throws InterruptedException;

    static TaskLimiter noop() {
        return topic -> () -> { };
    }

    static TaskLimiter bound(int size) {
        BlockingQueue<String> queue = new ArrayBlockingQueue<>(size);
        return topic -> {
            queue.put(topic);
            AtomicBoolean alive = new AtomicBoolean(true);
            return () -> {
                if (alive.getAndSet(false)) {
                    queue.remove(topic);
                }
            };
        };
    }

    @FunctionalInterface
    interface Token {

        void release();
    }
}
