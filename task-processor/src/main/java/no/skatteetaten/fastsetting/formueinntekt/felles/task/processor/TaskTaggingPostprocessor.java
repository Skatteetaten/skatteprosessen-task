package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor;

import java.util.function.BiFunction;
import java.util.stream.Stream;

import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.Task;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskCreation;

public class TaskTaggingPostprocessor implements BiFunction<Task, TaskCreation, Stream<TaskCreation>> {

    private final BiFunction<Task, TaskCreation, String> referent;

    private final boolean override;

    public TaskTaggingPostprocessor() {
        referent = (task, creation) -> Long.toString(task.getSequence());
        override = true;
    }

    public TaskTaggingPostprocessor(BiFunction<Task, TaskCreation, String> referent) {
        this.referent = referent;
        override = true;
    }

    public TaskTaggingPostprocessor(BiFunction<Task, TaskCreation, String> referent, boolean override) {
        this.referent = referent;
        this.override = override;
    }

    @Override
    public Stream<TaskCreation> apply(Task task, TaskCreation creation) {
        return Stream.of(override || creation.getReference().isEmpty()
            ? creation.withReference(referent.apply(task, creation))
            : creation);
    }
}
