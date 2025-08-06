package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor;

import org.junit.Test;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

public class PrioritizingTaskLimiterTest {

    @Test
    public void prioritizes_tasks() throws Exception {
        TaskLimiter limiter = new PrioritizingTaskLimiter(3, Integer::parseInt);
        Queue<TaskLimiter.Token> tokens = new ArrayDeque<>();
        tokens.add(limiter.request("1"));
        tokens.add(limiter.request("2"));
        tokens.add(limiter.request("3"));
        CountDownLatch started4 = new CountDownLatch(1), pushed4 = new CountDownLatch(1);
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        new Thread(() -> {
            try {
                started4.countDown();
                limiter.request("4");
                pushed4.countDown();
            } catch (Throwable t) {
                thrown.set(t);
            }
        }).start();
        CountDownLatch started5 = new CountDownLatch(1), pushed5 = new CountDownLatch(1);
        new Thread(() -> {
            try {
                started4.await(500, TimeUnit.MILLISECONDS);
                Thread.sleep(100);
                started5.countDown();
                limiter.request("5");
                pushed5.countDown();
            } catch (Throwable t) {
                thrown.set(t);
            }
        }).start();
        try {
            try {
                assertThat(started4.await(500, TimeUnit.MILLISECONDS)).isTrue();
                assertThat(started5.await(500, TimeUnit.MILLISECONDS)).isTrue();
                Thread.sleep(100);
                assertThat(pushed4.getCount()).isEqualTo(1);
                assertThat(pushed5.getCount()).isEqualTo(1);
            } finally {
                tokens.remove().release();
            }
            assertThat(pushed5.await(500, TimeUnit.MILLISECONDS)).isTrue();
            assertThat(pushed4.getCount()).isEqualTo(1);
            assertThat(thrown.get()).isNull();
        } finally {
            tokens.remove().release();
        }
        assertThat(pushed4.await(500, TimeUnit.MILLISECONDS)).isTrue();
        assertThat(thrown.get()).isNull();
    }
}