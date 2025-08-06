package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.*;
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

    @Mock
    private TaskJuncture<Void, RuntimeException> taskJunctor;

    @Mock
    private TaskProcessor taskProcessor;

    @Test
    public void can_push_task() throws Exception {
        TaskConsumerContext<Void, RuntimeException, TaskSupplement> context = new TaskConsumerContext<>(
            Collections.singleton(new Task(123, "1")),
            taskSink,
            taskJunctor,
            topic -> topic.equals("topic") ? Optional.of(taskProcessor) : Optional.empty()
        );

        context.pushByTask(new Task(123, "1"), "topic", TaskSink.Insertion.APPEND);

        when(taskSink.push(
            null, "topic", TaskSink.Insertion.APPEND, Collections.singletonList(new TaskCreation("1")))
        ).thenReturn(Collections.singletonList(new Task(456, "1")));

        TaskSupplement supplement = new TaskSupplement();
        CompletionStage<TaskCompletion<Void, RuntimeException>> stage = context.apply(
            Collections.singletonMap(new Task(123, "1"), TaskDecision.SUCCESS),
            Runnable::run,
            supplement
        );
        assertThat(stage).isCompleted();

        stage.toCompletableFuture().get().complete(null);
        verify(taskSink).push(null, "topic", TaskSink.Insertion.APPEND, Collections.singletonList(new TaskCreation("1")));

        assertThat(supplement.require(TaskConsumerContext.TopicsToResume.class).getTopics()).containsExactly("topic");
        verifyNoInteractions(taskProcessor);

        verifyNoInteractions(taskJunctor);
    }

    @Test
    public void can_resume_topics() {
        TaskConsumerContext<Void, RuntimeException, TaskSupplement> context = new TaskConsumerContext<>(
            Collections.singleton(new Task(123, "1")),
            taskSink,
            taskJunctor,
            topic -> topic.equals("topic") ? Optional.of(taskProcessor) : Optional.empty()
        );

        TaskSupplement supplement = new TaskSupplement();
        supplement.register(TaskConsumerContext.TopicsToResume.class, new TaskConsumerContext.TopicsToResume(Set.of("topic", "other")));
        context.onAfterTransaction(Runnable::run, supplement);
        verify(taskProcessor).resume();

        verifyNoInteractions(taskJunctor);
    }
}
