package no.skatteetaten.fastsetting.formueinntekt.felles.task.api;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(Parameterized.class)
public class TaskStateTest {

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {TaskState.ACTIVE, 0},
            {TaskState.READY, 1},
            {TaskState.EXPIRED, 2},
            {TaskState.SUCCEEDED, 3},
            {TaskState.SUSPENDED, 4},
            {TaskState.FILTERED, 5},
            {TaskState.FAILED, 6},
            {TaskState.RECREATED, 7},
            {TaskState.REDUNDANT, 8}
        });
    }

    private final TaskState state;

    private final int ordinal;

    public TaskStateTest(TaskState state, int ordinal) {
        this.state = state;
        this.ordinal = ordinal;
    }

    @Test
    public void defines_expected_ordinal() {
        assertThat(state.ordinal()).isEqualTo(ordinal);
    }

    @Test
    public void resolves_ordinal() {
        assertThat(TaskState.ofOrdinal(ordinal)).isEqualTo(state);
    }
}
