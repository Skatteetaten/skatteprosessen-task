package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.Task;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskDecision;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskResult;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskResultException;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskSupplement;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class CompositeTaskHandlerFactoryTest {

    @Parameterized.Parameters(name = "concurrency = {0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{{0}, {1}, {5}});
    }

    private final int concurrency;

    private ExecutorService executorService;

    public CompositeTaskHandlerFactoryTest(int concurrency) {
        this.concurrency = concurrency;
    }

    @Before
    public void setUp() {
        executorService = Executors.newSingleThreadExecutor(job -> {
            Thread thread = new Thread(job);
            thread.setName("composite-task-consumer-factory-test");
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
        try (TaskHandler<?, Exception> consumer = new CompositeTaskHandlerFactory<Void, Exception, String, TaskSupplement, TaskContext<Void, Exception, TaskSupplement>>(
            executorService,
            false,
            concurrency,
            (topic, tasks) -> (decisions, executor, supplement) -> CompletableFuture.completedStage(transaction -> decisions),
            context -> new TaskSupplement()
        ).apply("topic")) {
            CountDownLatch latch = new CountDownLatch(1);
            consumer.accept(
                Set.of(new Task(10, "foo"), new Task(15, "bar"), new Task(20, "qux")),
                completion -> {
                    assertThat(completion.complete(null)).containsOnlyKeys(
                        new Task(10, "foo"), new Task(15, "bar"), new Task(20, "qux")
                    ).containsValues(TaskDecision.SUCCESS);
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
    public void can_complete_tasks_exception_transaction() throws Exception {
        try (TaskHandler<?, Exception> consumer = new CompositeTaskHandlerFactory<Void, Exception, String, TaskSupplement, TaskContext<Void, Exception, TaskSupplement>>(
            executorService,
            false,
            concurrency,
            (topic, tasks) -> (decisions, executor, supplement) -> CompletableFuture.completedStage(transaction -> {
                throw new RuntimeException();
            }),
            context -> new TaskSupplement()
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

    @Test
    public void can_complete_tasks_exceptionally_transaction() throws Exception {
        try (TaskHandler<?, Exception> consumer = new CompositeTaskHandlerFactory<Void, Exception, String, TaskSupplement, TaskContext<Void, Exception, TaskSupplement>>(
            executorService,
            false,
            concurrency,
            (topic , tasks) -> (decisions, executor, supplement) -> CompletableFuture.failedStage(new RuntimeException()),
            context -> new TaskSupplement()
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

    @Test
    public void can_complete_tasks_exception_processor() throws Exception {
        try (TaskHandler<?, Exception> consumer = new CompositeTaskHandlerFactory<Void, Exception, String, TaskSupplement, TaskContext<Void, Exception, TaskSupplement>>(
            executorService,
            false,
            concurrency,
            (topic, tasks) -> (decisions, executor, supplement) -> {
                throw new RuntimeException();
            },
            context -> new TaskSupplement()
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

    @Test
    public void can_complete_tasks_regular_singular_step() throws Exception {
        ConcurrentMap<Task, List<String>> steps = new ConcurrentHashMap<>();
        try (TaskHandler<?, Exception> consumer = new CompositeTaskHandlerFactory<Void, Exception, String, TaskSupplement, TaskContext<Void, Exception, TaskSupplement>>(
            executorService,
            false,
            concurrency,
            (topic, tasks) -> (decisions, executor, supplement) -> CompletableFuture.completedStage(transaction -> decisions),
            context -> new TaskSupplement()
        ).withEachNoResult(
            "1", (task, context) -> steps.computeIfAbsent(task, ignored -> new CopyOnWriteArrayList<>()).add("1")
        ).apply("topic")) {
            CountDownLatch latch = new CountDownLatch(1);
            consumer.accept(
                Set.of(new Task(10, "foo"), new Task(15, "bar"), new Task(20, "qux")),
                completion -> {
                    assertThat(completion.complete(null)).containsOnlyKeys(
                        new Task(10, "foo"), new Task(15, "bar"), new Task(20, "qux")
                    ).containsValues(TaskDecision.SUCCESS);
                    latch.countDown();
                },
                throwable -> {
                    throw new AssertionError(throwable);
                }
            );
            assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();
        }
        assertThat(steps)
            .containsOnlyKeys(new Task(10, "foo"), new Task(15, "bar"), new Task(20, "qux"))
            .containsValues(Collections.singletonList("1"));
    }

    @Test
    public void can_complete_tasks_regular_singular_step_exception() throws Exception {
        try (TaskHandler<?, Exception> consumer = new CompositeTaskHandlerFactory<Void, Exception, String, TaskSupplement, TaskContext<Void, Exception, TaskSupplement>>(
            executorService,
            false,
            concurrency,
            (topic, tasks) -> (decisions, executor, supplement) -> CompletableFuture.completedStage(transaction -> decisions),
            context -> new TaskSupplement()
        ).withEachNoResult(
            "1", (task, context) -> {
                throw new RuntimeException();
            }
        ).apply("topic")) {
            CountDownLatch latch = new CountDownLatch(1);
            consumer.accept(
                Set.of(new Task(10, "foo"), new Task(15, "bar"), new Task(20, "qux")),
                completion -> {
                    assertThat(completion.complete(null)).containsOnlyKeys(
                        new Task(10, "foo"), new Task(15, "bar"), new Task(20, "qux")
                    ).containsValues(TaskDecision.FAILURE);
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
    public void can_complete_tasks_regular_singular_step_processing_exception() throws Exception {
        try (TaskHandler<?, Exception> consumer = new CompositeTaskHandlerFactory<Void, Exception, String, TaskSupplement, TaskContext<Void, Exception, TaskSupplement>>(
            executorService,
            false,
            concurrency,
            (topic, tasks) -> (decisions, executor, supplement) -> CompletableFuture.completedStage(transaction -> decisions),
            context -> new TaskSupplement()
        ).withEachNoResult(
            "1", (task, context) -> {
                throw new TaskResultException(TaskResult.FILTER);
            }
        ).apply("topic")) {
            CountDownLatch latch = new CountDownLatch(1);
            consumer.accept(
                Set.of(new Task(10, "foo"), new Task(15, "bar"), new Task(20, "qux")),
                completion -> {
                    assertThat(completion.complete(null)).containsOnlyKeys(
                        new Task(10, "foo"), new Task(15, "bar"), new Task(20, "qux")
                    ).containsValues(TaskDecision.FILTER);
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
    public void can_complete_tasks_regular_step() throws Exception {
        ConcurrentMap<Task, List<String>> steps = new ConcurrentHashMap<>();
        try (TaskHandler<?, Exception> consumer = new CompositeTaskHandlerFactory<Void, Exception, String, TaskSupplement, TaskContext<Void, Exception, TaskSupplement>>(
            executorService,
            false,
            concurrency,
            (topic, tasks) -> (decisions, executor, supplement) -> CompletableFuture.completedStage(transaction -> decisions),
            context -> new TaskSupplement()
        ).withNoResult(
            "1", (tasks, context) -> tasks.forEach(task -> steps.computeIfAbsent(task, ignored -> new CopyOnWriteArrayList<>()).add("1"))
        ).apply("topic")) {
            CountDownLatch latch = new CountDownLatch(1);
            consumer.accept(
                Set.of(new Task(10, "foo"), new Task(15, "bar"), new Task(20, "qux")),
                completion -> {
                    assertThat(completion.complete(null)).containsOnlyKeys(
                        new Task(10, "foo"), new Task(15, "bar"), new Task(20, "qux")
                    ).containsValues(TaskDecision.SUCCESS);
                    latch.countDown();
                },
                throwable -> {
                    throw new AssertionError(throwable);
                }
            );
            assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();
        }
        assertThat(steps)
            .containsOnlyKeys(new Task(10, "foo"), new Task(15, "bar"), new Task(20, "qux"))
            .containsValues(Collections.singletonList("1"));
    }

    @Test
    public void can_complete_tasks_regular_step_exception() throws Exception {
        try (TaskHandler<?, Exception> consumer = new CompositeTaskHandlerFactory<Void, Exception, String, TaskSupplement, TaskContext<Void, Exception, TaskSupplement>>(
            executorService,
            false,
            concurrency,
            (topic, tasks) -> (decisions, executor, supplement) -> CompletableFuture.completedStage(transaction -> decisions),
            context -> new TaskSupplement()
        ).withNoResult(
            "1", (tasks, context) -> {
                throw new RuntimeException();
            }
        ).apply("topic")) {
            CountDownLatch latch = new CountDownLatch(1);
            consumer.accept(
                Set.of(new Task(10, "foo"), new Task(15, "bar"), new Task(20, "qux")),
                completion -> {
                    assertThat(completion.complete(null)).containsOnlyKeys(
                        new Task(10, "foo"), new Task(15, "bar"), new Task(20, "qux")
                    ).containsValues(TaskDecision.FAILURE);
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
    public void can_complete_tasks_regular_step_processing_exception() throws Exception {
        try (TaskHandler<?, Exception> consumer = new CompositeTaskHandlerFactory<Void, Exception, String, TaskSupplement, TaskContext<Void, Exception, TaskSupplement>>(
            executorService,
            false,
            concurrency,
            (topic, tasks) -> (decisions, executor, supplement) -> CompletableFuture.completedStage(transaction -> decisions),
            context -> new TaskSupplement()
        ).withNoResult(
            "1", (tasks, context) -> {
                throw new TaskResultException(TaskResult.FILTER);
            }
        ).apply("topic")) {
            CountDownLatch latch = new CountDownLatch(1);
            consumer.accept(
                Set.of(new Task(10, "foo"), new Task(15, "bar"), new Task(20, "qux")),
                completion -> {
                    assertThat(completion.complete(null)).containsOnlyKeys(
                        new Task(10, "foo"), new Task(15, "bar"), new Task(20, "qux")
                    ).containsValues(TaskDecision.FILTER);
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
    public void can_complete_tasks_regular_singular_step_chain() throws Exception {
        ConcurrentMap<Task, List<String>> steps = new ConcurrentHashMap<>();
        try (TaskHandler<?, Exception> consumer = new CompositeTaskHandlerFactory<Void, Exception, String, TaskSupplement, TaskContext<Void, Exception, TaskSupplement>>(
            executorService,
            false,
            concurrency,
            (topic, tasks) -> (decisions, executor, supplement) -> CompletableFuture.completedStage(transaction -> decisions),
            context -> new TaskSupplement()
        ).withEachNoResult(
            "1", (task, context) -> {
                steps.computeIfAbsent(task, ignored -> new CopyOnWriteArrayList<>()).add("1");
                if (task.getIdentifier().equals("foo")) {
                    throw new RuntimeException();
                }
            }
        ).withEachNoResult(
            "2", (task, context) -> {
                steps.computeIfAbsent(task, ignored -> new CopyOnWriteArrayList<>()).add("2");
                if (task.getIdentifier().equals("bar")) {
                    throw new RuntimeException();
                }
            }, "1"
        ).withEachNoResult(
            "3", (task, context) -> steps.computeIfAbsent(task, ignored -> new CopyOnWriteArrayList<>()).add("3"), "2"
        ).apply("topic")) {
            CountDownLatch latch = new CountDownLatch(1);
            consumer.accept(
                Set.of(new Task(10, "foo"), new Task(15, "bar"), new Task(20, "qux")),
                completion -> {
                    assertThat(completion.complete(null)).containsOnlyKeys(
                        new Task(10, "foo"), new Task(15, "bar"), new Task(20, "qux")
                    ).containsValues(TaskDecision.SUCCESS);
                    latch.countDown();
                },
                throwable -> {
                    throw new AssertionError(throwable);
                }
            );
            assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();
        }
        assertThat(steps).containsOnlyKeys(new Task(10, "foo"), new Task(15, "bar"), new Task(20, "qux"));
        assertThat(steps.get(new Task(10, "foo"))).containsExactly("1");
        assertThat(steps.get(new Task(15, "bar"))).containsExactly("1", "2");
        assertThat(steps.get(new Task(20, "qux"))).containsExactly("1", "2", "3");
    }

    @Test(timeout = 2_500)
    public void fails_pending_on_close() throws Exception {
        if (concurrency == 0) {
            return;
        }
        CountDownLatch latch = new CountDownLatch(1);
        try (TaskHandler<?, Exception> consumer = new CompositeTaskHandlerFactory<Void, Exception, String, TaskSupplement, TaskContext<Void, Exception, TaskSupplement>>(
            executorService,
            true,
            concurrency,
            (topic, tasks) -> (decisions, executor, supplement) -> CompletableFuture.completedStage(transaction -> decisions),
            context -> new TaskSupplement()
        ).withAsync("1", (task, context) -> {
            latch.countDown();
            return new CompletableFuture<>();
        }).apply("topic")) {
            consumer.accept(
                Set.of(new Task(10, "foo"), new Task(15, "bar"), new Task(20, "qux")),
                completion -> {
                    throw new AssertionError();
                },
                throwable -> {
                    assertThat(throwable).isInstanceOf(TaskHandlerShutdownException.class);
                    latch.countDown();
                }
            );
        }
        if (!latch.await(1, TimeUnit.SECONDS)) {
            throw new AssertionError();
        }
    }

    @Test(timeout = 2_500)
    public void awaits_execution_on_close() throws Exception {
        if (concurrency == 0) {
            return;
        }
        CountDownLatch start = new CountDownLatch(1), finish = new CountDownLatch(1);
        try (TaskHandler<?, Exception> consumer = new CompositeTaskHandlerFactory<Void, Exception, String, TaskSupplement, TaskContext<Void, Exception, TaskSupplement>>(
            executorService,
            false,
            concurrency,
            (topic, tasks) -> (decisions, executor, supplement) -> CompletableFuture.completedStage(transaction -> decisions),
            context -> new TaskSupplement()
        ).withNoResult("1", (task, context) -> {
            try {
                assertThat(start.await(1, TimeUnit.SECONDS)).isTrue();
                Thread.sleep(250);
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        }).apply("topic")) {
            consumer.accept(
                Set.of(new Task(10, "foo"), new Task(15, "bar"), new Task(20, "qux")),
                completion -> {
                    assertThat(completion.complete(null)).containsOnlyKeys(
                        new Task(10, "foo"), new Task(15, "bar"), new Task(20, "qux")
                    ).containsValues(TaskDecision.SUCCESS);
                    finish.countDown();
                },
                throwable -> {
                    throw new AssertionError(throwable);
                }
            );
            start.countDown();
        }
        assertThat(finish.await(1, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void cannot_use_unknown_dependency() {
        assertThatThrownBy(() -> new CompositeTaskHandlerFactory<Void, Exception, String, TaskSupplement, TaskContext<Void, Exception, TaskSupplement>>(
            executorService,
            false,
            concurrency,
            TaskContext.<Void, Exception, TaskSupplement>simple().toFactory(),
            context -> new TaskSupplement()
        ).withEachNoResult("1", (task, context) -> { }, "2")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void cannot_reuse_identifier() {
        assertThatThrownBy(() -> new CompositeTaskHandlerFactory<Void, Exception, String, TaskSupplement, TaskContext<Void, Exception, TaskSupplement>>(
            executorService,
            false,
            concurrency,
            TaskContext.<Void, Exception, TaskSupplement>simple().toFactory(),
            context -> new TaskSupplement()
        ).withEachNoResult("1", (task, context) -> { }).withEachNoResult("1", (task, context) -> { })).isInstanceOf(IllegalArgumentException.class);
    }
}
