package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.Task;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskDecision;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class TaskManagerTest {

    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    private ExecutorService executorService;

    @Mock
    private TaskSource<?, Exception> source;

    @Before
    public void setUp() {
        executorService = Executors.newSingleThreadExecutor(job -> {
            Thread thread = new Thread(job);
            thread.setName("task-manager-test");
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
    public void can_complete_task_regular() throws Exception {
        TaskProcessor processor = new TaskManager<>(
            "topic",
            executorService,
            source,
            TaskDispatcher.simple(() -> null),
            TaskLimiter.noop(),
            topic -> (tasks, callback, onFailure) -> callback.accept(transaction -> tasks.stream().collect(Collectors.toMap(
                Function.identity(), task -> TaskDecision.SUCCESS
            ))),
            TaskListener.onFatal(throwable -> {
                throw new AssertionError(throwable);
            }),
            TaskSource.Order.FIRST_IN_FIRST_OUT,
            TaskSource.Condition.NONE,
            10,
            100,
            TimeUnit.MILLISECONDS
        );

        when(source.poll(null, "topic", TaskSource.Order.FIRST_IN_FIRST_OUT, 10)).thenReturn(Set.of(
            new Task(10, "foo"), new Task(15, "bar"), new Task(20, "qux")
        )).thenReturn(Collections.emptySet());

        assertThat(processor.start(250, TimeUnit.MILLISECONDS)).isTrue();
        try {
            verify(source, timeout(500)).complete(null, "topic", Map.of(
                new Task(10, "foo"), TaskDecision.SUCCESS,
                new Task(15, "bar"), TaskDecision.SUCCESS,
                new Task(20, "qux"), TaskDecision.SUCCESS
            ));
        } finally {
            assertThat(processor.stop(250, TimeUnit.MILLISECONDS)).isTrue();
        }
    }

    @Test
    public void can_complete_task_exceptional() throws Exception {
        TaskProcessor processor = new TaskManager<>(
            "topic",
            executorService,
            source,
            TaskDispatcher.simple(() -> null),
            TaskLimiter.noop(),
            topic -> (tasks, callback, onFailure) -> {
                throw new RuntimeException();
            },
            TaskListener.onFatal(throwable -> {
                throw new AssertionError(throwable);
            }),
            TaskSource.Order.FIRST_IN_FIRST_OUT,
            TaskSource.Condition.NONE,
            10,
            100,
            TimeUnit.MILLISECONDS
        );

        when(source.poll(null, "topic", TaskSource.Order.FIRST_IN_FIRST_OUT, 10)).thenReturn(Set.of(
            new Task(10, "foo"), new Task(15, "bar"), new Task(20, "qux")
        )).thenReturn(Collections.emptySet());

        assertThat(processor.start(250, TimeUnit.MILLISECONDS)).isTrue();
        try {
            verify(source, timeout(500)).complete(null, "topic", Map.of(
                new Task(10, "foo"), TaskDecision.FAILURE,
                new Task(15, "bar"), TaskDecision.FAILURE,
                new Task(20, "qux"), TaskDecision.FAILURE
            ));
        } finally {
            assertThat(processor.stop(250, TimeUnit.MILLISECONDS)).isTrue();
        }
    }

    @Test
    public void can_complete_task_failure() throws Exception {
        TaskProcessor processor = new TaskManager<>(
            "topic",
            executorService,
            source,
            TaskDispatcher.simple(() -> null),
            TaskLimiter.noop(),
            topic -> (tasks, callback, onFailure) -> onFailure.accept(new RuntimeException()),
            TaskListener.onFatal(throwable -> {
                throw new AssertionError(throwable);
            }),
            TaskSource.Order.FIRST_IN_FIRST_OUT,
            TaskSource.Condition.NONE,
            10,
            100,
            TimeUnit.MILLISECONDS
        );

        when(source.poll(null, "topic", TaskSource.Order.FIRST_IN_FIRST_OUT, 10)).thenReturn(Set.of(
            new Task(10, "foo"), new Task(15, "bar"), new Task(20, "qux")
        )).thenReturn(Collections.emptySet());

        assertThat(processor.start(250, TimeUnit.MILLISECONDS)).isTrue();
        try {
            verify(source, timeout(500)).complete(null, "topic", Map.of(
                new Task(10, "foo"), TaskDecision.FAILURE,
                new Task(15, "bar"), TaskDecision.FAILURE,
                new Task(20, "qux"), TaskDecision.FAILURE
            ));
        } finally {
            assertThat(processor.stop(250, TimeUnit.MILLISECONDS)).isTrue();
        }
    }

    @Test
    public void can_complete_task_premature_exception() throws Exception {
        TaskProcessor processor = new TaskManager<>(
            "topic",
            executorService,
            source,
            TaskDispatcher.simple(() -> null),
            TaskLimiter.noop(),
            topic -> (tasks, callback, onFailure) -> {
                throw new RuntimeException();
            },
            TaskListener.onFatal(throwable -> {
                throw new AssertionError(throwable);
            }),
            TaskSource.Order.FIRST_IN_FIRST_OUT,
            TaskSource.Condition.NONE,
            10,
            100,
            TimeUnit.MILLISECONDS
        );

        when(source.poll(null, "topic", TaskSource.Order.FIRST_IN_FIRST_OUT, 10)).thenReturn(Set.of(
            new Task(10, "foo"), new Task(15, "bar"), new Task(20, "qux")
        )).thenReturn(Collections.emptySet());

        assertThat(processor.start(250, TimeUnit.MILLISECONDS)).isTrue();
        try {
            verify(source, timeout(500)).complete(null, "topic", Map.of(
                new Task(10, "foo"), TaskDecision.FAILURE,
                new Task(15, "bar"), TaskDecision.FAILURE,
                new Task(20, "qux"), TaskDecision.FAILURE
            ));
        } finally {
            assertThat(processor.stop(250, TimeUnit.MILLISECONDS)).isTrue();
        }
    }

    @Test
    public void can_handle_transaction_error() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        TaskProcessor processor = new TaskManager<>(
            "topic",
            executorService,
            source,
            TaskDispatcher.simple(() -> {
                throw new RuntimeException();
            }),
            TaskLimiter.noop(),
            topic -> (tasks, callback, onFailure) -> {
                throw new AssertionError();
            },
            TaskListener.onFatal(throwable -> {
                assertThat(throwable).isInstanceOf(RuntimeException.class);
                latch.countDown();
            }),
            TaskSource.Order.FIRST_IN_FIRST_OUT,
            TaskSource.Condition.NONE,
            10,
            100,
            TimeUnit.MILLISECONDS
        );

        assertThat(processor.start(250, TimeUnit.MILLISECONDS)).isTrue();
        try {
            assertThat(latch.await(500, TimeUnit.MILLISECONDS)).isTrue();
        } finally {
            assertThat(processor.stop(250, TimeUnit.MILLISECONDS)).isTrue();
        }
    }

    @Test
    public void can_handle_poll_error() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        TaskProcessor processor = new TaskManager<>(
            "topic",
            executorService,
            source,
            TaskDispatcher.simple(() -> null),
            TaskLimiter.noop(),
            topic -> (tasks, callback, onFailure) -> {
                throw new AssertionError();
            },
            TaskListener.onFatal(throwable -> {
                assertThat(throwable).isInstanceOf(RuntimeException.class);
                latch.countDown();
            }),
            TaskSource.Order.FIRST_IN_FIRST_OUT,
            TaskSource.Condition.NONE,
            10,
            100,
            TimeUnit.MILLISECONDS
        );

        when(source.poll(null, "topic", TaskSource.Order.FIRST_IN_FIRST_OUT, 10)).thenThrow(new RuntimeException());

        assertThat(processor.start(250, TimeUnit.MILLISECONDS)).isTrue();
        try {
            assertThat(latch.await(500, TimeUnit.MILLISECONDS)).isTrue();
        } finally {
            assertThat(processor.stop(250, TimeUnit.MILLISECONDS)).isTrue();
        }
    }

    @Test
    public void can_handle_completion_error() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        TaskProcessor processor = new TaskManager<>(
            "topic",
            executorService,
            source,
            TaskDispatcher.simple(() -> null),
            TaskLimiter.noop(),
            topic -> (tasks, callback, onFailure) -> {
                throw new AssertionError();
            },
            TaskListener.onFatal(throwable -> {
                assertThat(throwable).isInstanceOf(RuntimeException.class);
                latch.countDown();
            }),
            TaskSource.Order.FIRST_IN_FIRST_OUT,
            TaskSource.Condition.NONE,
            10,
            100,
            TimeUnit.MILLISECONDS
        );

        when(source.poll(null, "topic", TaskSource.Order.FIRST_IN_FIRST_OUT, 10)).thenReturn(Set.of(
            new Task(10, "foo"), new Task(15, "bar"), new Task(20, "qux")
        ));

        doThrow(new RuntimeException()).when(source).complete(null, "topic", Map.of(
            new Task(10, "foo"), TaskDecision.FAILURE,
            new Task(15, "bar"), TaskDecision.FAILURE,
            new Task(20, "qux"), TaskDecision.FAILURE
        ));

        assertThat(processor.start(250, TimeUnit.MILLISECONDS)).isTrue();
        try {
            assertThat(latch.await(500, TimeUnit.MILLISECONDS)).isTrue();
        } finally {
            assertThat(processor.stop(250, TimeUnit.MILLISECONDS)).isTrue();
        }
    }

    @Test
    public void can_handle_limiter_error() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        TaskProcessor processor = new TaskManager<>(
            "topic",
            executorService,
            source,
            TaskDispatcher.simple(() -> null),
            topic -> {
                throw new RuntimeException();
            },
            topic -> (tasks, callback, onFailure) -> {
                throw new AssertionError();
            },
            TaskListener.onFatal(throwable -> {
                assertThat(throwable).isInstanceOf(RuntimeException.class);
                latch.countDown();
            }),
            TaskSource.Order.FIRST_IN_FIRST_OUT,
            TaskSource.Condition.NONE,
            10,
            100,
            TimeUnit.MILLISECONDS
        );

        assertThat(processor.start(250, TimeUnit.MILLISECONDS)).isTrue();
        assertThat(latch.await(500, TimeUnit.MILLISECONDS)).isTrue();
    }

    @Test
    public void can_resume() throws Exception {
        TaskProcessor processor = new TaskManager<>(
            "topic",
            executorService,
            source,
            TaskDispatcher.simple(() -> null),
            TaskLimiter.noop(),
            topic -> (tasks, callback, onFailure) -> callback.accept(transaction -> tasks.stream().collect(Collectors.toMap(
                Function.identity(), task -> TaskDecision.SUCCESS
            ))),
            TaskListener.onFatal(throwable -> {
                throw new AssertionError(throwable);
            }),
            TaskSource.Order.FIRST_IN_FIRST_OUT,
            TaskSource.Condition.NONE,
            10,
            25,
            TimeUnit.SECONDS
        );

        when(source.poll(null, "topic", TaskSource.Order.FIRST_IN_FIRST_OUT, 10)).thenReturn(Collections.emptySet()).thenReturn(Set.of(
            new Task(10, "foo"), new Task(15, "bar"), new Task(20, "qux")
        )).thenReturn(Collections.emptySet());

        assertThat(processor.start(250, TimeUnit.MILLISECONDS)).isTrue();
        try {
            verify(source, timeout(500)).poll(null, "topic", TaskSource.Order.FIRST_IN_FIRST_OUT, 10);
            verify(source, never()).complete(any(), anyString(), anyMap());
            assertThat(processor.resume()).isTrue();
            verify(source, timeout(500)).complete(null, "topic", Map.of(
                new Task(10, "foo"), TaskDecision.SUCCESS,
                new Task(15, "bar"), TaskDecision.SUCCESS,
                new Task(20, "qux"), TaskDecision.SUCCESS
            ));
        } finally {
            assertThat(processor.stop(250, TimeUnit.MILLISECONDS)).isTrue();
        }
    }

    @Test
    public void can_complete_task_regular_repeated() throws Exception {
        TaskProcessor processor = new TaskManager<>(
            "topic",
            executorService,
            source,
            TaskDispatcher.simple(() -> null),
            TaskLimiter.noop(),
            topic -> (tasks, callback, onFailure) -> callback.accept(transaction -> tasks.stream().collect(Collectors.toMap(
                Function.identity(), task -> TaskDecision.SUCCESS
            ))),
            TaskListener.onFatal(throwable -> {
                throw new AssertionError(throwable);
            }),
            TaskSource.Order.FIRST_IN_FIRST_OUT,
            TaskSource.Condition.NONE,
            10,
            100,
            TimeUnit.MILLISECONDS
        );

        when(source.poll(null, "topic", TaskSource.Order.FIRST_IN_FIRST_OUT, 10)).thenReturn(Set.of(
            new Task(10, "foo"), new Task(15, "bar"), new Task(20, "qux")
        )).thenReturn(Set.of(
            new Task(25, "foo"), new Task(30, "bar"), new Task(35, "qux")
        )).thenReturn(Collections.emptySet());

        assertThat(processor.start(250, TimeUnit.MILLISECONDS)).isTrue();
        try {
            verify(source, timeout(500)).complete(null, "topic", Map.of(
                new Task(10, "foo"), TaskDecision.SUCCESS,
                new Task(15, "bar"), TaskDecision.SUCCESS,
                new Task(20, "qux"), TaskDecision.SUCCESS
            ));
            verify(source, timeout(1_000)).complete(null, "topic", Map.of(
                new Task(25, "foo"), TaskDecision.SUCCESS,
                new Task(30, "bar"), TaskDecision.SUCCESS,
                new Task(35, "qux"), TaskDecision.SUCCESS
            ));
        } finally {
            assertThat(processor.stop(250, TimeUnit.MILLISECONDS)).isTrue();
        }
    }
}
