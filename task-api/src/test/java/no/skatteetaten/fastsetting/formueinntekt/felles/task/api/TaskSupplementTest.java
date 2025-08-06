package no.skatteetaten.fastsetting.formueinntekt.felles.task.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

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

    @Test
    public void can_register_and_read_token() {
        TaskSupplement supplement = new TaskSupplement();
        TaskSupplement.Token<List<String>> strings = new TaskSupplement.Token<>() { };
        supplement.register(strings, List.of("foo"));
        assertThat(supplement.suggest(strings, () -> {
            throw new AssertionError();
        })).containsExactly("foo");
        assertThatThrownBy(() -> supplement.register(strings, List.of("bar"))).isInstanceOf(IllegalStateException.class);
        assertThat(supplement.require(strings)).containsExactly("foo");
        assertThat(supplement.probe(strings)).hasValue(List.of("foo"));
        TaskSupplement.Token<List<Integer>> integers = new TaskSupplement.Token<>() { };
        supplement.register(integers, List.of(42));
        assertThat(supplement.suggest(integers, () -> {
            throw new AssertionError();
        })).containsExactly(42);
        assertThatThrownBy(() -> supplement.register(integers, List.of(42))).isInstanceOf(IllegalStateException.class);
        assertThat(supplement.require(integers)).containsExactly(42);
        assertThat(supplement.probe(integers)).hasValue(List.of(42));
    }

    @Test
    public void cannot_build_token_hierarchies() {
        class MediumToken<T> extends TaskSupplement.Token<T> { }
        assertThatThrownBy(() -> new MediumToken<>() { }).isInstanceOf(IllegalStateException.class);
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void cannot_build_non_parameterized_token() {
        assertThatThrownBy(() -> new TaskSupplement.Token() { }).isInstanceOf(IllegalStateException.class);
    }
}
