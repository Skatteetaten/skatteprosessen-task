package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.Task;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskDecision;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskResult;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskSupplement;
import org.junit.Test;

public class CompositeTaskContextTest {

    @Test
    public void can_merge_successful() throws Exception {
        CompletionStage<TaskCompletion<Void, Exception>> completion = new CompositeTaskContext<Void, Exception, TaskSupplement>(
            (decisions, executor, supplement) -> CompletableFuture.completedStage(transaction -> decisions),
            (decisions, executor, supplement) -> CompletableFuture.completedStage(transaction -> decisions)
        ).apply(
            Collections.singletonMap(new Task(1, "foo"), TaskDecision.SUCCESS),
            Runnable::run,
            new TaskSupplement()
        );
        assertThat(completion.toCompletableFuture().get(500, TimeUnit.MILLISECONDS).complete(null)).isNotNull();
        assertThat(completion.toCompletableFuture().get(500, TimeUnit.MILLISECONDS)
            .complete(null)
            .get(new Task(1, "foo"))
            .getResult()).isEqualTo(TaskResult.SUCCESS);
    }

    @Test
    public void can_merge_failed() throws Exception {
        CompletionStage<TaskCompletion<Void, Exception>> completion = new CompositeTaskContext<Void, Exception, TaskSupplement>(
            (decisions, executor, supplement) -> CompletableFuture.completedStage(transaction -> decisions),
            (decisions, executor, supplement) -> CompletableFuture.failedStage(new RuntimeException())
        ).apply(
            Collections.singletonMap(new Task(1, "foo"), TaskDecision.SUCCESS),
            Runnable::run,
            new TaskSupplement()
        );
        assertThat(completion.toCompletableFuture().get(500, TimeUnit.MILLISECONDS)
            .complete(null)
            .get(new Task(1, "foo"))
            .getResult()).isEqualTo(TaskResult.FAILURE);
    }
}
