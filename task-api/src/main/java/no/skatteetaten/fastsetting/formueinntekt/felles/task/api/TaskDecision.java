package no.skatteetaten.fastsetting.formueinntekt.felles.task.api;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TaskDecision {

    public static final TaskDecision FAILURE = new TaskDecision(TaskResult.FAILURE);
    public static final TaskDecision SUSPENSION = new TaskDecision(TaskResult.SUSPENSION);
    public static final TaskDecision FILTER = new TaskDecision(TaskResult.FILTER);
    public static final TaskDecision SUCCESS = new TaskDecision(TaskResult.SUCCESS);

    private final TaskResult result;
    private final Throwable throwable;
    private final boolean continued;

    private TaskDecision(TaskResult result) {
        this.result = result;
        throwable = null;
        continued = result == TaskResult.SUCCESS;
    }

    public TaskDecision(String message) {
        result = TaskResult.SUCCESS;
        throwable = message == null ? null : new MessageCarryingThrowable(message);
        continued = true;
    }

    public TaskDecision(Throwable throwable) {
        result = TaskResult.FAILURE;
        this.throwable = throwable;
        continued = false;
    }

    public TaskDecision(TaskResult result, String message) {
        this.result = result;
        throwable = message == null ? null : new MessageCarryingThrowable(message);
        continued = result == TaskResult.SUCCESS;
    }

    public TaskDecision(TaskResult result, Throwable throwable) {
        this.result = result;
        this.throwable = throwable;
        continued = result == TaskResult.SUCCESS;
    }

    TaskDecision(TaskResult result, Throwable throwable, boolean continued) {
        this.result = result;
        this.throwable = throwable;
        this.continued = continued;
    }

    public TaskResult getResult() {
        return result;
    }

    public Optional<Throwable> getThrowable() {
        return Optional.ofNullable(throwable);
    }

    public boolean isContinued() {
        return continued;
    }

    public TaskDecision withContinuation(boolean continued) {
        return this.continued == continued ? this : new TaskDecision(result, throwable, continued);
    }

    public TaskDecision merge(TaskDecision other) {
        if (result == other.result && throwable == other.throwable) {
            return this;
        } else {
            return new TaskDecision(
                result.merge(other.result),
                throwable == null || throwable == other.throwable
                    ? other.throwable
                    : other.throwable == null ? throwable : new MergedThrowable(throwable, other.throwable),
                continued && other.continued
            );
        }
    }

    public TaskDecision include(String message) {
        return message == null ? this : include(new MessageCarryingThrowable(message));
    }

    public TaskDecision include(Throwable throwable) {
        return new TaskDecision(
            result,
            this.throwable == null || this.throwable == throwable
                ? throwable
                : throwable == null ? this.throwable : new MergedThrowable(this.throwable, throwable),
            continued
        );
    }

    public Optional<String> toMessage() {
        if (throwable == null) {
            return Optional.empty();
        } else if (throwable instanceof MessageCarryingThrowable) {
            return Optional.of(throwable.getMessage());
        } else {
            StringWriter writer = new StringWriter();
            throwable.printStackTrace(new PrintWriter(writer, true));
            return Optional.of(writer.toString());
        }
    }

    public boolean isSameAs(TaskDecision decision) {
        if (result != decision.result) {
            return false;
        } else if (throwable == null) {
            return decision.throwable == null;
        } else if (throwable instanceof MessageCarryingThrowable) {
            return decision.throwable instanceof MessageCarryingThrowable && throwable.getMessage().equals(decision.throwable.getMessage());
        } else {
            return throwable.equals(decision.throwable);
        }
    }

    public static Map<Task, TaskDecision> combine(
        Map<Task, TaskDecision> left,
        Map<Task, TaskDecision> right
    ) {
        return Stream.concat(left.entrySet().stream(), right.entrySet().stream()).collect(Collectors.toMap(
            Map.Entry::getKey, Map.Entry::getValue
        ));
    }

    public static Map<Task, TaskDecision> merge(
        Map<Task, TaskDecision> left,
        Map<Task, TaskDecision> right
    ) {
        return left.keySet().stream().filter(right::containsKey).collect(Collectors.toMap(
            Function.identity(), task -> left.get(task).merge(right.get(task))
        ));
    }

    @Override
    public int hashCode() {
        return result.hashCode();
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        TaskDecision that = (TaskDecision) object;
        return result == that.result;
    }

    @Override
    public String toString() {
        return "task:decision:" + result + (throwable != null ? "/" + throwable.getClass().getTypeName() : "");
    }

    static class MergedThrowable extends Throwable {

        MergedThrowable(Throwable left, Throwable right) {
            super("Multiple stacks collected", null, true, false);
            if (left instanceof MergedThrowable) {
                Arrays.stream(left.getSuppressed()).forEach(this::addSuppressed);
            } else {
                addSuppressed(left);
            }
            if (right instanceof MergedThrowable) {
                Arrays.stream(right.getSuppressed()).forEach(this::addSuppressed);
            } else {
                addSuppressed(right);
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("Captured ").append(getSuppressed().length).append(" throwables:");
            Arrays.stream(getSuppressed()).forEach(throwable -> sb.append("\n")
                .append(throwable.getClass().getTypeName())
                .append(": ")
                .append(throwable.getMessage()));
            return sb.toString();
        }
    }

    static class MessageCarryingThrowable extends Throwable {

        MessageCarryingThrowable(String message) {
            super(message, null, false, false);
        }

        @Override
        public String toString() {
            return getMessage();
        }
    }
}
