package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor;

import org.junit.Test;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

public class BoundTaskLimiterTest {

    @Test
    public void limits_tasks() throws Exception {
        TaskLimiter limiter = TaskLimiter.bound(3);
        Queue<TaskLimiter.Token> tokens = new ArrayDeque<>();
        tokens.add(limiter.request("topic"));
        tokens.add(limiter.request("topic"));
        tokens.add(limiter.request("topic"));
        CountDownLatch started = new CountDownLatch(1), pushed = new CountDownLatch(1);
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        new Thread(() -> {
            try {
                started.countDown();
                limiter.request("topic");
                pushed.countDown();
            } catch (Throwable t) {
                thrown.set(t);
            }
        }).start();
        try {
            assertThat(started.await(500, TimeUnit.MILLISECONDS)).isTrue();
            Thread.sleep(100);
            assertThat(pushed.getCount()).isEqualTo(1);
        } finally {
            tokens.remove().release();
        }
        assertThat(pushed.await(500, TimeUnit.MILLISECONDS)).isTrue();
        assertThat(thrown.get()).isNull();
    }

}