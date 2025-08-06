package no.skatteetaten.fastsetting.formueinntekt.felles.task.api;

import java.time.OffsetDateTime;
import java.util.Optional;

public class TaskInfo extends Task {

    private final TaskState state, descent;
    private final String owner, output, reference;
    private final OffsetDateTime created, completed;

    public TaskInfo(
        long sequence, String identifier,
        TaskState state, TaskState descent, String owner, String reference,
        OffsetDateTime created, OffsetDateTime completed,
        String input, String output
    ) {
        super(sequence, identifier, input);
        this.owner = owner;
        this.reference = reference;
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

    public Optional<String> getReference() {
        return Optional.ofNullable(reference);
    }

    public OffsetDateTime getCreated() {
        return created;
    }

    public Optional<OffsetDateTime> getCompleted() {
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
