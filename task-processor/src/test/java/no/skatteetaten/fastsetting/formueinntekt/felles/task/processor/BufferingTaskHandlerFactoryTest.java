package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.Task;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskDecision;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BufferingTaskHandlerFactoryTest {

    private ExecutorService executorService;

    @Before
    public void setUp() {
        executorService = Executors.newSingleThreadExecutor(job -> {
            Thread thread = new Thread(job);
            thread.setName("buffering-task-consumer-factory-test");
            thread.setDaemon(true);
            thread.setUncaughtExceptionHandler((t, exception) -> exception.printStackTrace());
            return thread;
        });
    }

    @After
    public void shutDown() throws InterruptedException {
        executorService.shutdownNow();
        assertThat(executorService.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void can_complete_tasks_regular() throws Exception {
        try (TaskHandler<?, Exception> consumer = BufferingTaskHandlerFactory.withoutBaggage(
            executorService,
            false,
            5,
            10,
            100,
            TimeUnit.MILLISECONDS,
            topic -> (tasks, callback, onFailure) -> callback.accept(transaction -> tasks.stream().collect(Collectors.toMap(
                Function.identity(), task -> TaskDecision.SUCCESS
            )))
        ).apply("topic")) {
            CountDownLatch latch = new CountDownLatch(1);
            consumer.accept(
                Set.of(new Task(10, "foo"), new Task(15, "bar"), new Task(20, "qux")),
                completion -> {
                    assertThat(completion.complete(null)).containsOnlyKeys(
                        new Task(10, "foo"), new Task(15, "bar"), new Task(20, "qux")
                    );
                    latch.countDown();
                },
                throwable -> {
                    throw new AssertionError(throwable);
                }
            );
            assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    public void can_complete_tasks_exceptional() throws Exception {
        try (TaskHandler<?, Exception> consumer = BufferingTaskHandlerFactory.withoutBaggage(
            executorService,
            false,
            5,
            10,
            100,
            TimeUnit.MILLISECONDS,
            topic -> (tasks, callback, onFailure) -> {
                throw new RuntimeException();
            }
        ).apply("topic")) {
            CountDownLatch latch = new CountDownLatch(1);
            consumer.accept(
                Set.of(new Task(10, "foo"), new Task(15, "bar"), new Task(20, "qux")),
                completion -> {
                    throw new AssertionError();
                },
                throwable -> {
                    assertThat(throwable).isInstanceOf(RuntimeException.class);
                    latch.countDown();
                }
            );
            assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test(timeout = 2_500)
    public void on_shutdown_fail_pending() throws Exception {
        CountDownLatch start = new CountDownLatch(1), finish = new CountDownLatch(2);
        try (TaskHandler<?, Exception> consumer = BufferingTaskHandlerFactory.withoutBaggage(
            executorService,
            true,
            5,
            1,
            100,
            TimeUnit.MILLISECONDS,
            topic -> (tasks, callback, onFailure) -> {
                start.countDown();
                Thread.sleep(Long.MAX_VALUE);
            }
        ).apply("topic")) {
            consumer.accept(
                Set.of(new Task(10, "foo")),
                completion -> {
                    throw new AssertionError();
                },
                throwable -> {
                    assertThat(throwable).isInstanceOf(InterruptedException.class);
                    finish.countDown();
                }
            );
            consumer.accept(
                Set.of(new Task(15, "bar")),
                completion -> {
                    throw new AssertionError();
                },
                throwable -> {
                    assertThat(throwable).isInstanceOf(TaskHandlerShutdownException.class);
                    finish.countDown();
                }
            );
            assertThat(start.await(1, TimeUnit.SECONDS)).isTrue();
        }
        assertThat(finish.await(1, TimeUnit.SECONDS)).isTrue();
    }

    @Test(timeout = 2_500)
    public void on_shutdown_complete_pending() throws Exception {
        CountDownLatch start = new CountDownLatch(1), finish = new CountDownLatch(2);
        try (TaskHandler<?, Exception> consumer = BufferingTaskHandlerFactory.withoutBaggage(
            executorService,
            false,
            5,
            1,
            100,
            TimeUnit.MILLISECONDS,
            topic -> (tasks, callback, onFailure) -> {
                if (tasks.contains(new Task(10, "foo"))) {
                    start.countDown();
                    Thread.sleep(Long.MAX_VALUE);
                } else {
                    callback.accept(transaction -> tasks.stream().collect(Collectors.toMap(
                        Function.identity(),
                        task -> TaskDecision.SUCCESS
                    )));
                }
            }
        ).apply("topic")) {
            consumer.accept(
                Set.of(new Task(10, "foo")),
                completion -> {
                    throw new AssertionError();
                },
                throwable -> {
                    assertThat(throwable).isInstanceOf(InterruptedException.class);
                    finish.countDown();
                }
            );
            consumer.accept(
                Set.of(new Task(15, "bar")),
                completion -> {
                    assertThat(completion.complete(null)).isEqualTo(Collections.singletonMap(
                        new Task(15, "bar"),
                        TaskDecision.SUCCESS
                    ));
                    finish.countDown();
                },
                throwable -> {
                    throw new AssertionError(throwable);
                }
            );
        }
        assertThat(finish.await(1, TimeUnit.SECONDS)).isTrue();
    }

    @Test(timeout = 2_500)
    public void can_shutdown_long_poll() throws Exception {
        try (TaskHandler<?, Exception> ignored = BufferingTaskHandlerFactory.withoutBaggage(
            executorService,
            true,
            5,
            10,
            Long.MAX_VALUE,
            TimeUnit.MILLISECONDS,
            topic -> (tasks, callback, onFailure) -> {
                throw new AssertionError();
            }
        ).apply("topic")) {
            Thread.sleep(250);
        }
    }
}
