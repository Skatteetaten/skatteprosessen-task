package no.skatteetaten.fastsetting.formueinntekt.felles.task.api;

import java.time.LocalDateTime;
import java.util.Optional;

public class TaskInfo extends Task {

    private final TaskState state, descent;
    private final String owner, output;
    private final LocalDateTime created, completed;

    public TaskInfo(
        long sequence, String identifier,
        TaskState state, TaskState descent, String owner,
        LocalDateTime created, LocalDateTime completed,
        String input, String output
    ) {
        super(sequence, identifier, input);
        this.owner = owner;
        this.state = state;
        this.descent = descent;
        this.created = created;
        this.completed = completed;
        this.output = output;
    }

    public TaskState getState() {
        return state;
    }

    public TaskState getDescent() {
        return descent;
    }

    public Optional<String> getOwner() {
        return Optional.ofNullable(owner);
    }

    public LocalDateTime getCreated() {
        return created;
    }

    public Optional<LocalDateTime> getCompleted() {
        return Optional.ofNullable(completed);
    }

    public Optional<String> getOutput() {
        return Optional.ofNullable(output);
    }

    @Override
    public String toString() {
        return "task.info:" + getIdentifier() + "@" + getSequence() + "/" + state;
    }
}
