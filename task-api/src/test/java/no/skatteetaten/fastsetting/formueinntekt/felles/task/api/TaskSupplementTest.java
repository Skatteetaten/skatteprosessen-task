package no.skatteetaten.fastsetting.formueinntekt.felles.task.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.junit.Test;

public class TaskSupplementTest {

    @Test
    public void can_register_and_read() {
        TaskSupplement supplement = new TaskSupplement();
        supplement.register(String.class, "foo");
        assertThat(supplement.suggest(String.class, () -> {
            throw new AssertionError();
        })).isEqualTo("foo");
        assertThatThrownBy(() -> supplement.register(String.class, "bar")).isInstanceOf(IllegalStateException.class);
        assertThat(supplement.require(String.class)).isEqualTo("foo");
        assertThat(supplement.probe(String.class)).hasValue("foo");
    }

    @Test
    public void respects_qualifier() {
        TaskSupplement supplement = new TaskSupplement();
        supplement.register(String.class, "foo");
        supplement.register(String.class, "qualifier", "bar");
        assertThat(supplement.probe(String.class)).hasValue("foo");
        assertThat(supplement.probe(String.class, "qualifier")).hasValue("bar");
        assertThat(supplement.probe(String.class, "other")).isEmpty();
    }

    @Test
    public void respects_task() {
        TaskSupplement supplement = new TaskSupplement();
        supplement.register(String.class, "foo");
        supplement.register(String.class, new Task(1, "1"), "bar");
        assertThat(supplement.probe(String.class)).hasValue("foo");
        assertThat(supplement.probe(String.class, new Task(1, "1"))).hasValue("bar");
        assertThat(supplement.probe(String.class, new Task(2, "1"))).isEmpty();
    }

    @Test
    public void respects_task_with_qualifier() {
        TaskSupplement supplement = new TaskSupplement();
        supplement.register(String.class, "foo");
        supplement.register(String.class, new Task(1, "1"), "bar");
        supplement.register(String.class, "qualifier", "qux");
        supplement.register(String.class, new Task(1, "1"), "qualifier", "baz");
        assertThat(supplement.probe(String.class)).hasValue("foo");
        assertThat(supplement.probe(String.class, new Task(1, "1"))).hasValue("bar");
        assertThat(supplement.probe(String.class, "qualifier")).hasValue("qux");
        assertThat(supplement.probe(String.class, new Task(1, "1"), "qualifier")).hasValue("baz");
        assertThat(supplement.probe(String.class, new Task(1, "1"), "other")).isEmpty();
        assertThat(supplement.probe(String.class, new Task(2, "1"), "qualifier")).isEmpty();
    }
}
