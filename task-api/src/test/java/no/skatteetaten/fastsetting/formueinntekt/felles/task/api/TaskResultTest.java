package no.skatteetaten.fastsetting.formueinntekt.felles.task.api;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(Parameterized.class)
public class TaskResultTest {

    @Parameterized.Parameters(name = "{0} + {1} = {2}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
            {TaskResult.FAILURE, TaskResult.FAILURE, TaskResult.FAILURE},
            {TaskResult.FAILURE, TaskResult.SUSPENSION, TaskResult.FAILURE},
            {TaskResult.FAILURE, TaskResult.FILTER, TaskResult.FAILURE},
            {TaskResult.FAILURE, TaskResult.SUCCESS, TaskResult.FAILURE},
            {TaskResult.SUSPENSION, TaskResult.FAILURE, TaskResult.FAILURE},
            {TaskResult.SUSPENSION, TaskResult.SUSPENSION, TaskResult.SUSPENSION},
            {TaskResult.SUSPENSION, TaskResult.FILTER, TaskResult.SUSPENSION},
            {TaskResult.SUSPENSION, TaskResult.SUCCESS, TaskResult.SUSPENSION},
            {TaskResult.FILTER, TaskResult.FAILURE, TaskResult.FAILURE},
            {TaskResult.FILTER, TaskResult.SUSPENSION, TaskResult.SUSPENSION},
            {TaskResult.FILTER, TaskResult.FILTER, TaskResult.FILTER},
            {TaskResult.FILTER, TaskResult.SUCCESS, TaskResult.FILTER},
            {TaskResult.SUCCESS, TaskResult.FAILURE, TaskResult.FAILURE},
            {TaskResult.SUCCESS, TaskResult.SUSPENSION, TaskResult.SUSPENSION},
            {TaskResult.SUCCESS, TaskResult.FILTER, TaskResult.FILTER},
            {TaskResult.SUCCESS, TaskResult.SUCCESS, TaskResult.SUCCESS}
        });
    }

    private final TaskResult left, right, result;

    public TaskResultTest(TaskResult left, TaskResult right, TaskResult result) {
        this.left = left;
        this.right = right;
        this.result = result;
    }

    @Test
    public void can_merge_results() {
        assertThat(left.merge(right)).isEqualTo(result);
    }
}