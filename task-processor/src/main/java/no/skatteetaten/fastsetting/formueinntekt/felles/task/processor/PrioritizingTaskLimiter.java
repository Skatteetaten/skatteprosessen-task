package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToIntFunction;

public class PrioritizingTaskLimiter implements TaskLimiter {

    private final Semaphore semaphore;
    private final ToIntFunction<String> prioritizer;

    private final AtomicLong sequencer = new AtomicLong();

    private final BlockingQueue<Unit> units = new PriorityBlockingQueue<>();

    public PrioritizingTaskLimiter(int concurrency, ToIntFunction<String> prioritizer) {
        semaphore = new Semaphore(concurrency);
        this.prioritizer = prioritizer;
    }

    @Override
    public Token request(String topic) throws InterruptedException {
        if (!semaphore.tryAcquire()) {
            Unit unit = new Unit(prioritizer.applyAsInt(topic), sequencer.getAndIncrement());
            units.put(unit);
            unit.latch.await();
        }
        AtomicBoolean alive = new AtomicBoolean(true);
        return () -> {
            if (alive.getAndSet(false)) {
                Unit queued = units.poll();
                if (queued == null) {
                    semaphore.release();
                } else {
                    queued.latch.countDown();
                }
            }
        };
    }

    static class Unit implements Comparable<Unit> {

        private final int priority;
        private final long sequence;

        private final CountDownLatch latch = new CountDownLatch(1);

        Unit(int priority, long sequence) {
            this.priority = priority;
            this.sequence = sequence;
        }

        @Override
        public int compareTo(Unit other) {
            int prioritization = Integer.compare(other.priority, priority);
            return prioritization == 0 ? Long.compare(sequence, other.sequence) : prioritization;
        }
    }
}
