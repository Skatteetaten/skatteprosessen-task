package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.*;

public class TaskConsumerContext<TRANSACTION, EXCEPTION extends Exception, SUPPLEMENT extends TaskSupplement> implements TaskConsumer, TaskContext<TRANSACTION, EXCEPTION, SUPPLEMENT> {

    private final Set<Task> tasks;

    private final TaskSink<TRANSACTION, EXCEPTION> taskSink;
    private final TaskJuncture<TRANSACTION, EXCEPTION> taskJuncture;

    private final Function<String, Optional<TaskProcessor>> processors;
    private final BiFunction<Task, TaskCreation, Stream<TaskCreation>> postprocessor;

    private final Map<String, List<Definition>> definitions = new ConcurrentHashMap<>();
    private final Map<String, List<Junction>> junctions = new ConcurrentHashMap<>();

    public TaskConsumerContext(
        Set<Task> tasks,
        TaskSink<TRANSACTION, EXCEPTION> taskSink,
        TaskJuncture<TRANSACTION, EXCEPTION> taskJuncture,
        Function<String, Optional<TaskProcessor>> processors
    ) {
        this.tasks = tasks;
        this.taskSink = taskSink;
        this.taskJuncture = taskJuncture;
        this.processors = processors;
        postprocessor = (task, creation) -> Stream.of(creation);
    }

    public TaskConsumerContext(
        Set<Task> tasks,
        TaskSink<TRANSACTION, EXCEPTION> taskSink,
        TaskJuncture<TRANSACTION, EXCEPTION> taskJuncture,
        Function<String, Optional<TaskProcessor>> processors,
        BiFunction<Task, TaskCreation, Stream<TaskCreation>> postprocessor
    ) {
        this.tasks = tasks;
        this.taskSink = taskSink;
        this.taskJuncture = taskJuncture;
        this.processors = processors;
        this.postprocessor = postprocessor;
    }

    @Override
    public void pushByTask(
        Set<Task> tasks,
        String topic,
        TaskSink.Insertion insertion,
        Function<Task, Collection<TaskCreation>> resolver
    ) {
        if (tasks.isEmpty()) {
            return;
        } else if (!this.tasks.containsAll(tasks)) {
            throw new IllegalArgumentException("Cannot write tasks for unknown tasks: " + tasks.stream()
                .filter(task -> !this.tasks.contains(task))
                .collect(Collectors.toList()));
        }
        definitions.computeIfAbsent(topic, ignored -> new CopyOnWriteArrayList<>()).add(new Definition(tasks, insertion, resolver));
    }

    @Override
    public void junctionByTask(Map<Task, Set<String>> tasksToIdentifiers, String topic, TaskJuncture.Junction junction) {
        if (tasksToIdentifiers.isEmpty()) {
            return;
        } else if (!tasks.containsAll(tasksToIdentifiers.keySet())) {
            throw new IllegalArgumentException("Cannot write tasks for unknown tasks: " + tasksToIdentifiers.keySet().stream()
                    .filter(task -> !tasks.contains(task))
                    .collect(Collectors.toList()));
        }
        junctions.computeIfAbsent(topic, ignored -> new CopyOnWriteArrayList<>()).add(new Junction(tasksToIdentifiers, junction));
    }

    @Override
    public CompletionStage<TaskCompletion<TRANSACTION, EXCEPTION>> apply(
        Map<Task, TaskDecision> decisions,
        Executor executor,
        SUPPLEMENT supplement
    ) {
        CompletableFuture<TaskCompletion<TRANSACTION, EXCEPTION>> future = new CompletableFuture<>();
        if (definitions.isEmpty() && junctions.isEmpty()) {
            future.complete(transaction -> decisions);
        } else {
            executor.execute(() -> {
                TransactionConsumer<TRANSACTION, EXCEPTION> pusher, junctor;
                Set<String> topics = new HashSet<>();
                try {
                    Set<Task> succeeded = decisions.entrySet().stream()
                        .filter(entry -> entry.getValue().isContinued())
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toSet());
                    Map<String, List<ResolvedDefinition>> definitions = this.definitions.entrySet().stream().collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().stream()
                            .flatMap(definition -> definition.resolve(succeeded, postprocessor).stream())
                            .collect(Collectors.toList())
                    ));
                    if (definitions.values().stream().mapToLong(Collection::size).sum() == 0) {
                        pusher = transaction -> { };
                    } else {
                        topics.addAll(definitions.keySet());
                        pusher = transaction -> {
                            for (Map.Entry<String, List<ResolvedDefinition>> entry : definitions.entrySet()) {
                                Iterator<ResolvedDefinition> it = entry.getValue().iterator();
                                ResolvedDefinition current = it.next();
                                TaskSink.Insertion insertion = current.getInsertion();
                                List<TaskCreation> creations = new ArrayList<>(definitions.size());
                                creations.add(current.getCreation());
                                while (it.hasNext()) {
                                    current = it.next();
                                    if (current.getInsertion() != insertion) {
                                        taskSink.push(transaction, entry.getKey(), insertion, creations);
                                        insertion = current.getInsertion();
                                        creations.clear();
                                    }
                                    creations.add(current.getCreation());
                                }
                                taskSink.push(transaction, entry.getKey(), insertion, creations);
                            }
                        };
                    }
                    Map<String, List<ResolvedJunction>> junctions = this.junctions.entrySet().stream().collect(Collectors.toMap(
                            Map.Entry::getKey,
                            entry -> entry.getValue().stream()
                                .map(junction -> junction.resolve(succeeded))
                                .collect(Collectors.toList())
                    ));
                    if (junctions.values().stream().mapToLong(Collection::size).sum() == 0) {
                        junctor = transaction -> { };
                    } else {
                        topics.addAll(junctions.keySet());
                        junctor = transaction -> {
                            for (Map.Entry<String, List<ResolvedJunction>> entry : junctions.entrySet()) {
                                Iterator<ResolvedJunction> it = entry.getValue().iterator();
                                ResolvedJunction current = it.next();
                                TaskJuncture.Junction junction = current.getJunction();
                                Set<Set<String>> groups = new HashSet<>(junctions.size());
                                groups.addAll(current.getGroups());
                                while (it.hasNext()) {
                                    current = it.next();
                                    if (current.getJunction() != junction) {
                                        taskJuncture.junction(transaction, entry.getKey(), junction, groups);
                                        junction = current.getJunction();
                                        groups.clear();
                                    }
                                    groups.addAll(current.getGroups());
                                }
                                taskJuncture.junction(transaction, entry.getKey(), junction, groups);
                            }
                        };
                    }
                    if (!topics.isEmpty()) {
                        supplement.register(TopicsToResume.class, new TopicsToResume(topics));
                    }
                    future.complete(transaction -> {
                        pusher.accept(transaction);
                        junctor.accept(transaction);
                        return decisions;
                    });
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                }

            });
        }
        return future;
    }

    @Override
    public void onAfterTransaction(Executor executor, SUPPLEMENT supplement) {
        supplement.probe(TopicsToResume.class).ifPresent(resume -> executor.execute(
            () -> resume.getTopics().forEach(topic -> processors.apply(topic).ifPresent(TaskProcessor::resume))
        ));
    }

    static class Definition {

        private final Set<Task> tasks;
        private final TaskSink.Insertion insertion;
        private final Function<Task, Collection<TaskCreation>> resolver;

        Definition(
            Set<Task> tasks,
            TaskSink.Insertion insertion,
            Function<Task, Collection<TaskCreation>> resolver
        ) {
            this.tasks = tasks;
            this.insertion = insertion;
            this.resolver = resolver;
        }

        List<ResolvedDefinition> resolve(Set<Task> succeeded, BiFunction<Task, TaskCreation, Stream<TaskCreation>> postprocessor) {
            return tasks.stream().filter(succeeded::contains).flatMap(task -> resolver.apply(task).stream()
                .flatMap(creation -> postprocessor.apply(task, creation).map(transformed -> new ResolvedDefinition(insertion, transformed))))
                .collect(Collectors.toList());
        }
    }

    static class ResolvedDefinition {

        private final TaskSink.Insertion insertion;
        private final TaskCreation creation;

        ResolvedDefinition(TaskSink.Insertion insertion, TaskCreation creation) {
            this.creation = creation;
            this.insertion = insertion;
        }


        TaskSink.Insertion getInsertion() {
            return insertion;
        }

        TaskCreation getCreation() {
            return creation;
        }
    }

    static class Junction {

        private final Map<Task, Set<String>> tasksToIdentifiers;
        private final TaskJuncture.Junction junction;

        Junction(Map<Task, Set<String>> tasksToIdentifiers, TaskJuncture.Junction junction) {
            this.tasksToIdentifiers = tasksToIdentifiers;
            this.junction = junction;
        }

        ResolvedJunction resolve(Set<Task> succeeded) {
            return new ResolvedJunction(junction, tasksToIdentifiers.entrySet().stream()
                    .filter(entry -> succeeded.contains(entry.getKey()))
                    .map(entry -> {
                        Set<String> identifiers = new HashSet<>(entry.getValue());
                        identifiers.add(entry.getKey().getIdentifier());
                        return identifiers;
                    }).collect(Collectors.toList()));
        }
    }

    static class ResolvedJunction {

        private final TaskJuncture.Junction junction;
        private final Collection<Set<String>> groups;

        ResolvedJunction(TaskJuncture.Junction junction, Collection<Set<String>> groups) {
            this.junction = junction;
            this.groups = groups;
        }

        TaskJuncture.Junction getJunction() {
            return junction;
        }

        Collection<Set<String>> getGroups() {
            return groups;
        }
    }

    static class TopicsToResume {

        private final Set<String> topics;

        TopicsToResume(Set<String> topics) {
            this.topics = topics;
        }

        Set<String> getTopics() {
            return topics;
        }
    }

    interface TransactionConsumer<TRANSACTION, EXCEPTION extends Exception> {

        void accept(TRANSACTION transaction) throws EXCEPTION;
    }
}
