package no.skatteetaten.fastsetting.formueinntekt.felles.task.api;

import java.util.Map;
import java.util.Optional;

public interface TaskReviver<TRANSACTION, EXCEPTION extends Exception> {

    default Optional<Task> recreate(
        TRANSACTION transaction, String topic, Revivification revivification, Revived revived
    ) throws EXCEPTION {
        return recreate(transaction, topic, revivification, revived, 1).values().stream().findFirst();
    }

    default Map<Task, Task> recreate(
        TRANSACTION transaction, String topic, Revivification revivification, Revived revived, int size
    ) throws EXCEPTION {
        return recreate(transaction, topic, revivification, revived, TaskRepository.INCEPTION, Long.MAX_VALUE, size);
    }

    Map<Task, Task> recreate(
        TRANSACTION transaction, String topic, Revivification revivification, Revived revived, long from, long to, int size
    ) throws EXCEPTION;

    enum Revivification {
        APPEND,
        REPLACE,
        SUPERSEDE,
        RESET
    }

    enum Revived {

        EXPIRED(TaskState.EXPIRED),
        SUCCEEDED(TaskState.SUCCEEDED),
        SUSPENDED(TaskState.SUSPENDED),
        FILTERED(TaskState.FILTERED),
        FAILED(TaskState.FAILED);

        private final TaskState state;

        Revived(TaskState state) {
            this.state = state;
        }

        public static Revived of(TaskResult result) {
            switch (result) {
            case SUCCESS:
                return SUCCEEDED;
            case SUSPENSION:
                return SUSPENDED;
            case FILTER:
                return FILTERED;
            case FAILURE:
                return FAILED;
            default:
                throw new IllegalStateException();
            }
        }

        public TaskState toState() {
            return state;
        }
    }
}
