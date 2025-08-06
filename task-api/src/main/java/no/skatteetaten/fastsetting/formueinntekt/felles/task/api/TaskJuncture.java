package no.skatteetaten.fastsetting.formueinntekt.felles.task.api;

import java.util.Collection;
import java.util.List;
import java.util.Set;

public interface TaskJuncture<TRANSACTION, EXCEPTION extends Exception> {

    default boolean junction(
        TRANSACTION transaction, String topic, Junction junction, String... identifiers
    ) throws EXCEPTION {
        return !junction(transaction, topic, junction, List.of(Set.of(identifiers))).isEmpty();
    }

    Set<String> junction(
        TRANSACTION transaction, String topic, Junction junction, Collection<Set<String>> groups
    ) throws EXCEPTION;

    enum Junction {
        SINGULAR,
        ETERNAL
    }
}
