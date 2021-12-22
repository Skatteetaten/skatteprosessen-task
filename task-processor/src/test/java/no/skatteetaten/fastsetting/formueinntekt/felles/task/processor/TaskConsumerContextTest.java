package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;

import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.Task;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskCreation;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskDecision;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskSink;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class TaskConsumerContextTest {

    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    @Mock
    private TaskSink<Void, RuntimeException> taskSink;

    @Test
    public void can_push_task() {
        TaskConsumerContext<Void, RuntimeException> context = new TaskConsumerContext<>(
            Collections.singleton(new Task(123, "1")),
            TaskDispatcher.simple(() -> null),
            taskSink,
            topic -> Optional.empty()
        );

        context.pushByTask(new Task(123, "1"), "topic", TaskSink.Insertion.APPEND);

        when(taskSink.push(
            null, "topic", TaskSink.Insertion.APPEND, Collections.singletonList(new TaskCreation("1")))
        ).thenReturn(Collections.singletonList(new Task(456, "1")));

        assertThat(context.apply(Collections.singletonMap(new Task(123, "1"), TaskDecision.SUCCESS), Runnable::run)).isCompleted();

        verify(taskSink).push(null, "topic", TaskSink.Insertion.APPEND, Collections.singletonList(new TaskCreation("1")));
    }
}
