package no.skatteetaten.fastsetting.formueinntekt.felles.task.api;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Task implements Comparable<Task> {

    private final long sequence;
    private final String identifier, input;

    public Task(long sequence, String identifier) {
        this.sequence = sequence;
        this.identifier = identifier;
        input = null;
    }

    public Task(long sequence, String identifier, String input) {
        this.sequence = sequence;
        this.identifier = identifier;
        this.input = input;
    }

    public static BiFunction<Set<Task>, TaskSupplement, Map<Task, TaskDecision>> filterDuplicatedIdentifiers() {
        return filterDuplicatedIdentifiers(TaskResult.FILTER);
    }

    public static BiFunction<Set<Task>, TaskSupplement, Map<Task, TaskDecision>> filterDuplicatedIdentifiers(TaskResult result) {
        return (tasks, supplement) -> {
            Map<String, Long> maxima = tasks.stream().collect(Collectors.toMap(Task::getIdentifier, Task::getSequence, Math::max));
            return tasks.stream().collect(Collectors.toMap(
                Function.identity(),
                task -> maxima.get(task.getIdentifier()) == task.getSequence()
                    ? TaskDecision.SUCCESS
                    : new TaskDecision(result, "Identifier is duplicated in batch")
            ));
        };
    }

    public long getSequence() {
        return sequence;
    }

    public String getIdentifier() {
        return identifier;
    }

    public Optional<String> getInput() {
        return Optional.ofNullable(input);
    }

    @Override
    public int compareTo(Task task) {
        return Long.compare(sequence, task.sequence);
    }

    @Override
    public int hashCode() {
        return (int) (sequence ^ (sequence >>> 32));
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof Task)) {
            return false;
        }
        Task task = (Task) object;
        if (sequence != task.sequence) {
            return false;
        }
        return identifier.equals(task.identifier);
    }

    @Override
    public String toString() {
        return "task:" + identifier + "@" + sequence;
    }
}
