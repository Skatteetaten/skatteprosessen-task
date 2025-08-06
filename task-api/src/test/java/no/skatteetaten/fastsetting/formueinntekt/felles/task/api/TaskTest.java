package no.skatteetaten.fastsetting.formueinntekt.felles.task.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Set;

import org.junit.Test;

public class TaskTest {

    @Test
    public void can_check_duplicates() {
        assertThat(Task.filterDuplicatedIdentifiers().apply(Set.of(
            new Task(123, "1"),
            new Task(456, "2"),
            new Task(789, "1")
        ), new TaskSupplement())).containsExactlyInAnyOrderEntriesOf(Map.of(
            new Task(123, "1"), TaskDecision.FILTER,
            new Task(456, "2"), TaskDecision.SUCCESS,
            new Task(789, "1"), TaskDecision.SUCCESS
        ));
    }
}
