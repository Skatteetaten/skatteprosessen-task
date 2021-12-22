package no.skatteetaten.fastsetting.formueinntekt.felles.task.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.junit.Test;

public class TaskDecisionTest {

    @Test
    public void can_extract_empty_result() {
        assertThat(TaskDecision.SUCCESS.toMessage()).isEmpty();
    }

    @Test
    public void can_extract_message() {
        assertThat(new TaskDecision("text").toMessage()).contains("text");
    }

    @Test
    public void can_extract_exception() {
        RuntimeException exception = new RuntimeException();
        StringWriter writer = new StringWriter();
        exception.printStackTrace(new PrintWriter(writer, true));
        assertThat(new TaskDecision(exception).toMessage()).contains(writer.toString());
    }

    @Test
    public void can_combine_exceptions() {
        assertThat(new TaskDecision(new RuntimeException("left")).merge(new TaskDecision(new RuntimeException("right"))).toMessage())
            .hasValueSatisfying(message -> assertThat(message).contains("left").contains("right"));
    }

    @Test
    public void can_include_message() {
        assertThat(new TaskDecision(new RuntimeException("left")).include("right").toMessage())
            .hasValueSatisfying(message -> assertThat(message).contains("left").contains("right"));
    }
}