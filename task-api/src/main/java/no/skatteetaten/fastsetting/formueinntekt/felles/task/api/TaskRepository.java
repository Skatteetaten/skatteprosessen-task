package no.skatteetaten.fastsetting.formueinntekt.felles.task.api;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public interface TaskRepository<TRANSACTION, EXCEPTION extends Exception> extends
    TaskSink<TRANSACTION, EXCEPTION>,
    TaskSource<TRANSACTION, EXCEPTION>,
    TaskReviver<TRANSACTION, EXCEPTION>,
    TaskJuncture<TRANSACTION, EXCEPTION> {

    long INCEPTION = 0;

    boolean register(TRANSACTION transaction) throws EXCEPTION;

    Set<String> owners(TRANSACTION transaction, long timeout, TimeUnit unit) throws EXCEPTION;

    void heartbeat(TRANSACTION transaction) throws EXCEPTION;

    void expire(TRANSACTION transaction, long timeout, TimeUnit unit) throws EXCEPTION;

    boolean initialize(TRANSACTION transaction, String topic) throws EXCEPTION;

    default void refresh(TRANSACTION transaction) throws EXCEPTION { }

    default void reassign(
        TRANSACTION transaction, String topic, Task task, TaskResult result
    ) throws EXCEPTION {
        reassign(transaction, topic, Collections.singletonMap(task, result));
    }

    void reassign(
        TRANSACTION transaction, String topic, Map<Task, TaskResult> tasks
    ) throws EXCEPTION;

    default long reassignAll(
        TRANSACTION transaction, String topic, Revived revived, TaskResult result
    ) throws EXCEPTION {
        return reassignAll(transaction, topic, revived, result, INCEPTION, Long.MAX_VALUE);
    }

    long reassignAll(
        TRANSACTION transaction, String topic, Revived revived, TaskResult result, long from, long to
    ) throws EXCEPTION;

    default Task recreate(
        TRANSACTION transaction, String topic, Revivification revivification, Task task
    ) throws EXCEPTION {
        return recreate(transaction, topic, revivification, Collections.singleton(task)).get(task);
    }

    <TASK extends Task> Map<TASK, Task> recreate(
        TRANSACTION transaction, String topic, Revivification revivification, Set<TASK> tasks
    ) throws EXCEPTION;

    default Optional<TaskInfo> task(
        TRANSACTION transaction, String topic, OffsetDateTime dateTime
    ) throws EXCEPTION {
        return task(transaction, topic, resolve(transaction, dateTime, false));
    }

    default Optional<TaskInfo> task(
        TRANSACTION transaction, String topic, long sequence
    ) throws EXCEPTION {
        if (sequence == INCEPTION) {
            return Optional.empty();
        }
        return page(transaction, topic, new Listing(), sequence + 1, 1, Direction.BACKWARD).stream().findFirst();
    }

    default List<TaskInfo> page(
        TRANSACTION transaction, String topic, OffsetDateTime dateTime, int size
    ) throws EXCEPTION {
        return page(transaction, topic, resolve(transaction, dateTime, false), size);
    }

    default List<TaskInfo> page(
        TRANSACTION transaction, String topic, long sequence, int size
    ) throws EXCEPTION {
        return page(transaction, topic, sequence, size, Direction.FORWARD);
    }

    default List<TaskInfo> page(
        TRANSACTION transaction, String topic, OffsetDateTime dateTime, int size, Direction direction
    ) throws EXCEPTION {
        return page(transaction, topic, resolve(transaction, dateTime, false), size, direction);
    }

    default List<TaskInfo> page(
        TRANSACTION transaction, String topic, long sequence, int size, Direction direction
    ) throws EXCEPTION {
        return page(transaction, topic, new Listing(), sequence, size, direction);
    }

    default List<TaskInfo> page(
        TRANSACTION transaction, String topic, Listing listing, OffsetDateTime dateTime, int size
    ) throws EXCEPTION {
        return page(transaction, topic, listing, resolve(transaction, dateTime, false), size);
    }

    default List<TaskInfo> page(
        TRANSACTION transaction, String topic, Listing listing, long sequence, int size
    ) throws EXCEPTION {
        return page(transaction, topic, listing, sequence, size, Direction.FORWARD);
    }

    default List<TaskInfo> page(
        TRANSACTION transaction, String topic, Listing listing, OffsetDateTime dateTime, int size, Direction direction
    ) throws EXCEPTION {
        return page(transaction, topic, listing, resolve(transaction, dateTime, direction == Direction.BACKWARD), size, direction);
    }

    List<TaskInfo> page(
        TRANSACTION transaction, String topic, Listing listing, long sequence, int size, Direction direction
    ) throws EXCEPTION;

    Set<String> topics(TRANSACTION transaction) throws EXCEPTION;

    default Map<String, Map<TaskState, Summary>> count(
        TRANSACTION transaction, Snapshot snapshot
    ) throws EXCEPTION {
        return count(transaction, snapshot, new Counting());
    }

    default Map<String, Map<TaskState, Summary>> count(
        TRANSACTION transaction, Snapshot snapshot, long from, long to
    ) throws EXCEPTION {
        return count(transaction, snapshot, new Counting(), from, to);
    }

    default Map<String, Map<TaskState, Summary>> count(
        TRANSACTION transaction, Snapshot snapshot, OffsetDateTime from, OffsetDateTime to
    ) throws EXCEPTION {
        return count(transaction, snapshot, new Counting(), resolve(transaction, from, false), resolve(transaction, to, true));
    }

    default Map<String, Map<TaskState, Summary>> count(
        TRANSACTION transaction, Snapshot snapshot, Counting counting
    ) throws EXCEPTION {
        return count(transaction, snapshot, counting, INCEPTION, Long.MAX_VALUE);
    }

    default Map<String, Map<TaskState, Summary>> count(
        TRANSACTION transaction, Snapshot snapshot, Counting counting, OffsetDateTime from, OffsetDateTime to
    ) throws EXCEPTION {
        return count(transaction, snapshot, counting, resolve(transaction, from, false), resolve(transaction, to, true));
    }

    Map<String, Map<TaskState, Summary>> count(
        TRANSACTION transaction, Snapshot snapshot, Counting counting, long from, long to
    ) throws EXCEPTION;

    default Map<String, Map<TaskResult, Long>> results(
        TRANSACTION transaction
    ) throws EXCEPTION {
        return results(transaction, OffsetDateTime.MIN, OffsetDateTime.MAX);
    }

    Map<String, Map<TaskResult, Long>> results(
        TRANSACTION transaction, OffsetDateTime from, OffsetDateTime to
    ) throws EXCEPTION;

    boolean destroy(TRANSACTION transaction, String topic) throws EXCEPTION;

    void purgeAll(TRANSACTION transaction, String topic) throws EXCEPTION;

    default long purge(TRANSACTION transaction, String topic) throws EXCEPTION {
        return purge(transaction, topic, INCEPTION, Long.MAX_VALUE);
    }

    long purge(TRANSACTION transaction, String topic, long from, long to) throws EXCEPTION;

    default long purge(TRANSACTION transaction, String topic, TaskState state) throws EXCEPTION {
        return purge(transaction, topic, state, INCEPTION, Long.MAX_VALUE);
    }

    long purge(TRANSACTION transaction, String topic, TaskState state, long from, long to) throws EXCEPTION;

    int purgeOwners(TRANSACTION transaction, long timeout, TimeUnit unit) throws EXCEPTION;

    default long resolve(TRANSACTION transaction, LocalDate date, boolean preceding) throws EXCEPTION {
        return resolve(transaction, date, ZoneOffset.UTC, preceding);
    }

    default long resolve(TRANSACTION transaction, LocalDate date, ZoneOffset offset, boolean preceding) throws EXCEPTION {
        return resolve(transaction, (preceding ? date.plusDays(1) : date).atStartOfDay().atOffset(offset), preceding);
    }

    long resolve(TRANSACTION transaction, OffsetDateTime dateTime, boolean preceding) throws EXCEPTION;

    enum Direction {
        FORWARD,
        BACKWARD;

        public boolean isLimit(long sequence) {
            return sequence == (this == FORWARD ? INCEPTION : Long.MAX_VALUE);
        }

        public boolean isOutOfBound(long sequence) {
            return sequence < INCEPTION || sequence == (this == BACKWARD ? INCEPTION : Long.MAX_VALUE);
        }
    }

    enum Snapshot {
        RECENT,
        TOTAL
    }

    class Listing {

        private final TaskState state;
        private final String reference;
        private final String identifier;

        public Listing() {
            state = null;
            reference = null;
            identifier = null;
        }

        private Listing(TaskState state, String reference, String identifier) {
            this.state = state;
            this.reference = reference;
            this.identifier = identifier;
        }

        public Optional<TaskState> getState() {
            return Optional.ofNullable(state);
        }

        public Optional<String> getReference() {
            return Optional.ofNullable(reference);
        }

        public Optional<String> getIdentifier() {
            return Optional.ofNullable(identifier);
        }

        public Listing withState(TaskState state) {
            return new Listing(state, reference, identifier);
        }

        public Listing withReference(String reference) {
            return new Listing(state, reference, identifier);
        }

        public Listing withIdentifier(String identifier) {
            return new Listing(state, reference, identifier);
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (object == null || getClass() != object.getClass()) {
                return false;
            }
            Listing listing = (Listing) object;
            if (state != listing.state) {
                return false;
            }
            if (reference != null ? !reference.equals(listing.reference) : listing.reference != null) {
                return false;
            }
            return identifier != null ? identifier.equals(listing.identifier) : listing.identifier == null;
        }

        @Override
        public int hashCode() {
            int result = state != null ? state.hashCode() : 0;
            result = 31 * result + (reference != null ? reference.hashCode() : 0);
            result = 31 * result + (identifier != null ? identifier.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "task:listing[state=" + state + ",reference=" + reference + ",identifier=" + identifier + "]";
        }
    }

    class Counting {

        private final String topic;
        private final TaskState state;
        private final String reference;
        private final String identifier;

        public Counting() {
            topic = null;
            state = null;
            reference = null;
            identifier = null;
        }

        private Counting(String topic, TaskState state, String reference, String identifier) {
            this.topic = topic;
            this.state = state;
            this.reference = reference;
            this.identifier = identifier;
        }

        public Optional<String> getTopic() {
            return Optional.ofNullable(topic);
        }

        public Optional<TaskState> getState() {
            return Optional.ofNullable(state);
        }

        public Optional<String> getReference() {
            return Optional.ofNullable(reference);
        }

        public Optional<String> getIdentifier() {
            return Optional.ofNullable(identifier);
        }

        public Counting withTopic(String topic) {
            return new Counting(topic, state, reference, identifier);
        }

        public Counting withState(TaskState state) {
            return new Counting(topic, state, reference, identifier);
        }

        public Counting withReference(String reference) {
            return new Counting(topic, state, reference, identifier);
        }

        public Counting withIdentifier(String identifier) {
            return new Counting(topic, state, reference, identifier);
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (object == null || getClass() != object.getClass()) {
                return false;
            }
            Counting counting = (Counting) object;
            if (topic != null ? !topic.equals(counting.topic) : counting.topic != null) {
                return false;
            }
            if (state != counting.state) {
                return false;
            }
            if (reference != null ? !reference.equals(counting.reference) : counting.reference != null) {
                return false;
            }
            return identifier != null ? identifier.equals(counting.identifier) : counting.identifier == null;
        }

        @Override
        public int hashCode() {
            int result = topic != null ? topic.hashCode() : 0;
            result = 31 * result + (state != null ? state.hashCode() : 0);
            result = 31 * result + (reference != null ? reference.hashCode() : 0);
            result = 31 * result + (identifier != null ? identifier.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "task:counting[topic=" + topic + ",state=" + state + ",reference=" + reference + ",identifier=" + identifier + "]";
        }
    }

    class Summary {

        private final long count, bound;

        public Summary(long count, long bound) {
            this.count = count;
            this.bound = bound;
        }

        public long getCount() {
            return count;
        }

        public long getBound() {
            return bound;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (object == null || getClass() != object.getClass()) {
                return false;
            }
            Summary summary = (Summary) object;
            if (count != summary.count) {
                return false;
            }
            return bound == summary.bound;
        }

        @Override
        public int hashCode() {
            int result = (int) (count ^ (count >>> 32));
            result = 31 * result + (int) (bound ^ (bound >>> 32));
            return result;
        }

        @Override
        public String toString() {
            return "task:summary[count=" + count + ",bound=" + bound + "]";
        }
    }
}
