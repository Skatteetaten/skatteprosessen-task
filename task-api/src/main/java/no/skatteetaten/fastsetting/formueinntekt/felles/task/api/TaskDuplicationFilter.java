package no.skatteetaten.fastsetting.formueinntekt.felles.task.api;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class TaskDuplicationFilter<CRITERIA, SUPPLEMENT extends TaskSupplement> implements BiFunction<Set<Task>, SUPPLEMENT, Map<Task, TaskDecision>> {

    private final BiFunction<Task, ? super SUPPLEMENT, CRITERIA> identification;

    private final Selector<CRITERIA, ? super SUPPLEMENT> selector;

    private final TaskResult ifFiltered;

    public TaskDuplicationFilter(
        BiFunction<Task, ? super SUPPLEMENT, CRITERIA> identification,
        Selector<CRITERIA, ? super SUPPLEMENT> selector,
        TaskResult ifFiltered
    ) {
        this.identification = identification;
        this.selector = selector;
        this.ifFiltered = ifFiltered;
    }

    public static <SUPPLEMENT extends TaskSupplement> BiFunction<Set<Task>, SUPPLEMENT, Map<Task, TaskDecision>> all() {
        return all(TaskResult.FAILURE);
    }

    public static <SUPPLEMENT extends TaskSupplement> BiFunction<Set<Task>, SUPPLEMENT, Map<Task, TaskDecision>> all(
        TaskResult ifFiltered
    ) {
        return all((task, supplement) -> task.getIdentifier(), ifFiltered);
    }

    public static <SUPPLEMENT extends TaskSupplement> BiFunction<Set<Task>, SUPPLEMENT, Map<Task, TaskDecision>> all(
        BiFunction<Task, ? super SUPPLEMENT, ?> identification
    ) {
        return all(identification, TaskResult.FAILURE);
    }

    public static <SUPPLEMENT extends TaskSupplement> BiFunction<Set<Task>, SUPPLEMENT, Map<Task, TaskDecision>> all(
        BiFunction<Task, ? super SUPPLEMENT, ?> identification,
        TaskResult ifFiltered
    ) {
        return new TaskDuplicationFilter<>(identification, (tasks, supplement, criteria) -> List.of(), ifFiltered);
    }

    public static <SUPPLEMENT extends TaskSupplement> BiFunction<Set<Task>, SUPPLEMENT, Map<Task, TaskDecision>> outdated() {
        return outdated(TaskResult.FILTER);
    }

    public static <SUPPLEMENT extends TaskSupplement> BiFunction<Set<Task>, SUPPLEMENT, Map<Task, TaskDecision>> outdated(
        TaskResult ifFiltered
    ) {
        return outdated((task, supplement) -> task.getIdentifier(), ifFiltered);
    }

    public static <SUPPLEMENT extends TaskSupplement> BiFunction<Set<Task>, SUPPLEMENT, Map<Task, TaskDecision>> outdated(
        BiFunction<Task, ? super SUPPLEMENT, ?> identification
    ) {
        return outdated(identification, TaskResult.FILTER);
    }

    public static <SUPPLEMENT extends TaskSupplement> BiFunction<Set<Task>, SUPPLEMENT, Map<Task, TaskDecision>> outdated(
        BiFunction<Task, ? super SUPPLEMENT, ?> identification,
        TaskResult ifFiltered
    ) {
        return new TaskDuplicationFilter<>(identification, (tasks, supplement, criteria) -> List.of(tasks.stream().max(Comparator.naturalOrder()).orElseThrow()), ifFiltered);
    }

    @Override
    public Map<Task, TaskDecision> apply(Set<Task> tasks, SUPPLEMENT supplement) {
        Map<Task, TaskDecision> decisions = new HashMap<>();
        tasks.stream().collect(Collectors.groupingBy(
            task -> identification.apply(task, supplement),
            Collectors.toCollection(TreeSet::new)
        )).forEach((criteria, duplicates) -> {
            if (duplicates.size() == 1) {
                duplicates.forEach(task -> decisions.put(task, TaskDecision.SUCCESS));
            } else {
                Set<Task> selected = new TreeSet<>(selector.apply(duplicates, supplement, criteria));
                duplicates.forEach(task -> decisions.put(task, selected.contains(task)
                    ? TaskDecision.SUCCESS
                    : new TaskDecision(ifFiltered, "Found duplicated among " + duplicates + " with selected: " + selected)));
            }
        });
        return decisions;
    }

    @FunctionalInterface
    public interface Selector<CRITERIA, SUPPLEMENT extends TaskSupplement> {

        Collection<Task> apply(Set<Task> tasks, SUPPLEMENT supplement, CRITERIA criteria);
    }
}
