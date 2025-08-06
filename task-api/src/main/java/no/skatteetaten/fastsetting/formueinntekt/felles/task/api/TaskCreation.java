package no.skatteetaten.fastsetting.formueinntekt.felles.task.api;

import java.util.Optional;

public class TaskCreation {

    private final String identifier, input, reference;
    private final boolean suspended;

    public TaskCreation(String identifier) {
        this.identifier = identifier;
        input = null;
        reference = null;
        suspended = false;
    }

    public TaskCreation(String identifier, String input) {
        this.identifier = identifier;
        this.input = input == null || input.isBlank() ? null : input;
        reference = null;
        suspended = false;
    }

    private TaskCreation(String identifier, String input, String reference, boolean suspended) {
        this.identifier = identifier;
        this.input = input;
        this.reference = reference;
        this.suspended = suspended;
    }

    public TaskCreation withReference(String reference) {
        return new TaskCreation(identifier, input, reference == null || reference.isBlank() ? null : reference, suspended);
    }

    public TaskCreation withSuspension(boolean suspended) {
        return new TaskCreation(identifier, input, reference, suspended);
    }

    public String getIdentifier() {
        return identifier;
    }

    public Optional<String> getInput() {
        return Optional.ofNullable(input);
    }

    public Optional<String> getReference() {
        return Optional.ofNullable(reference);
    }

    public boolean isSuspended() {
        return suspended;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        TaskCreation that = (TaskCreation) object;
        if (!identifier.equals(that.identifier)) {
            return false;
        }
        return input != null ? input.equals(that.input) : that.input == null;
    }

    @Override
    public int hashCode() {
        int result = identifier.hashCode();
        result = 31 * result + (input != null ? input.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "task:creation:" + identifier + (input != null ? "/" + input.length() + "c" : "");
    }

}
