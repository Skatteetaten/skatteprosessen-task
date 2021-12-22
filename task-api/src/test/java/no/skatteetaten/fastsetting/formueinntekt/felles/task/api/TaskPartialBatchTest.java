package no.skatteetaten.fastsetting.formueinntekt.felles.task.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.Test;

public class TaskPartialBatchTest {

    @Test
    public void can_evaluate_task_with_partial_elements() throws Exception {
        Map<Task, TaskDecision> decisions = TaskPartialBatch.ofResult(
            (task, supplement) -> task.getSequence() == 1,
            (tasks, supplement) -> tasks.stream().collect(Collectors.toMap(Function.identity(), task -> TaskDecision.FILTER))
        ).apply(
            Set.of(new Task(1, "123"), new Task(2, "456")),
            new TaskSupplement()
        ).toCompletableFuture().get();

        assertThat(decisions).isEqualTo(Map.of(
            new Task(1, "123"), TaskDecision.FILTER,
            new Task(2, "456"), TaskDecision.SUCCESS
        ));
    }

    @Test
    public void does_not_invoke_delegate_on_empty_set() throws Exception {
        Map<Task, TaskDecision> decisions = TaskPartialBatch.ofResult(
            (task, supplement) -> false,
            (tasks, supplement) -> {
                throw new AssertionError();
            }
        ).apply(
            Set.of(new Task(1, "123"), new Task(2, "456")),
            new TaskSupplement()
        ).toCompletableFuture().get();

        assertThat(decisions).isEqualTo(Map.of(
            new Task(1, "123"), TaskDecision.SUCCESS,
            new Task(2, "456"), TaskDecision.SUCCESS
        ));
    }
}
