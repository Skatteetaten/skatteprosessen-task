package no.skatteetaten.fastsetting.formueinntekt.felles.task.api;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

public class TaskSupplement {

    public static final String UNQUALIFIED = null;
    public static final Task GLOBAL = null;

    private final ConcurrentMap<Key, Object> supplements = new ConcurrentHashMap<>();

    public <T> T require(Class<T> type) {
        return require(type, UNQUALIFIED);
    }

    public <T> T require(Class<T> type, Task task) {
        return require(type, task, UNQUALIFIED);
    }

    public <T> T require(Class<T> type, String qualifier) {
        return require(type, GLOBAL, qualifier);
    }

    public <T> T require(Class<T> type, Task task, String qualifier) {
        return probe(type, task, qualifier).orElseThrow(() -> new IllegalStateException("Could not find supplement of type " + type.getTypeName() + (qualifier != null
            ? " with qualifier " + qualifier
            : " without qualifier") + (task == null ? "" : (" for task " + task))));
    }

    public <T> T require(Token<T> token) {
        return require(token, UNQUALIFIED);
    }

    public <T> T require(Token<T> token, Task task) {
        return require(token, task, UNQUALIFIED);
    }

    public <T> T require(Token<T> token, String qualifier) {
        return require(token, GLOBAL, qualifier);
    }

    public <T> T require(Token<T> token, Task task, String qualifier) {
        return probe(token, task, qualifier).orElseThrow(() -> new IllegalStateException("Could not find supplement of type " + token.getType().getTypeName() + (qualifier != null
            ? " with qualifier " + qualifier
            : " without qualifier") + (task == null ? "" : (" for task " + task))));
    }

    public <T> Optional<T> probe(Class<T> type) {
        return probe(type, UNQUALIFIED);
    }

    public <T> Optional<T> probe(Class<T> type, Task task) {
        return probe(type, task, UNQUALIFIED);
    }

    public <T> Optional<T> probe(Class<T> type, String qualifier) {
        return probe(type, GLOBAL, qualifier);
    }

    public <T> Optional<T> probe(Class<T> type, Task task, String qualifier) {
        return Optional.ofNullable(supplements.get(new Key(type, task, qualifier))).map(type::cast);
    }

    public <T> Optional<T> probe(Token<T> token) {
        return probe(token, UNQUALIFIED);
    }

    public <T> Optional<T> probe(Token<T> token, Task task) {
        return probe(token, task, UNQUALIFIED);
    }

    public <T> Optional<T> probe(Token<T> token, String qualifier) {
        return probe(token, GLOBAL, qualifier);
    }

    public <T> Optional<T> probe(Token<T> token, Task task, String qualifier) {
        return Optional.ofNullable(supplements.get(new Key(token.getType(), task, qualifier))).map(token::cast);
    }

    public <T> void register(Class<T> type, T value) {
        register(type, UNQUALIFIED, value);
    }

    public <T> void register(Class<T> type, Task task, T value) {
        register(type, task, UNQUALIFIED, value);
    }

    public <T> void register(Class<T> type, String qualifier, T value) {
        register(type, GLOBAL, qualifier, value);
    }

    public <T> void register(Class<T> type, Task task, String qualifier, T value) {
        if (supplements.putIfAbsent(new Key(type, task, qualifier), value) != null) {
            throw new IllegalStateException("Supplement for " + type.getTypeName() + " "
                + (qualifier == null ? "without qualifier" : ("with qualifier '" + qualifier + "'"))
                + " already registered" + (task == null ? "" : (" for task " + task)));
        }
    }

    public <T> void register(Token<T> token, T value) {
        register(token, UNQUALIFIED, value);
    }

    public <T> void register(Token<T> token, Task task, T value) {
        register(token, task, UNQUALIFIED, value);
    }

    public <T> void register(Token<T> token, String qualifier, T value) {
        register(token, GLOBAL, qualifier, value);
    }

    public <T> void register(Token<T> token, Task task, String qualifier, T value) {
        if (supplements.putIfAbsent(new Key(token.getType(), task, qualifier), value) != null) {
            throw new IllegalStateException("Supplement for " + token.getType().getTypeName() + " "
                + (qualifier == null ? "without qualifier" : ("with qualifier '" + qualifier + "'"))
                + " already registered" + (task == null ? "" : (" for task " + task)));
        }
    }

    public <T> T suggest(Class<T> type, Supplier<? extends T> factory) {
        return suggest(type, UNQUALIFIED, factory);
    }

    public <T> T suggest(Class<T> type, Task task, Supplier<? extends T> factory) {
        return suggest(type, task, UNQUALIFIED, factory);
    }

    public <T> T suggest(Class<T> type, String qualifier, Supplier<? extends T> factory) {
        return suggest(type, GLOBAL, qualifier, factory);
    }

    public <T> T suggest(Class<T> type, Task task, String qualifier, Supplier<? extends T> factory) {
        return type.cast(supplements.computeIfAbsent(new Key(type, task, qualifier), ignored -> factory.get()));
    }

    public <T> T suggest(Token<T> token, Supplier<? extends T> factory) {
        return suggest(token, UNQUALIFIED, factory);
    }

    public <T> T suggest(Token<T> token, Task task, Supplier<? extends T> factory) {
        return suggest(token, task, UNQUALIFIED, factory);
    }

    public <T> T suggest(Token<T> token, String qualifier, Supplier<? extends T> factory) {
        return suggest(token, GLOBAL, qualifier, factory);
    }

    public <T> T suggest(Token<T> token, Task task, String qualifier, Supplier<? extends T> factory) {
        return token.cast(supplements.computeIfAbsent(new Key(token.getType(), task, qualifier), ignored -> factory.get()));
    }

    @SuppressWarnings("unused")
    public static abstract class Token<T> {

        private final Type type;

        protected Token() {
            if (getClass().getSuperclass() != Token.class) {
                throw new IllegalStateException("Expected a direct subclass of " + Token.class.getTypeName());
            }
            Type type = getClass().getGenericSuperclass();
            if (!(type instanceof ParameterizedType)) {
                throw new IllegalStateException("Expected subclass of " + Token.class.getTypeName() + " to be parameterized");
            }
            this.type = ((ParameterizedType) type).getActualTypeArguments()[0];
        }

        Type getType() {
            return type;
        }
        
        @SuppressWarnings("unchecked")
        <S> S cast(Object value) {
            return (S) value;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (object == null || getClass() != object.getClass()) {
                return false;
            }
            Token<?> token = (Token<?>) object;
            return type.equals(token.type);
        }

        @Override
        public int hashCode() {
            return type.hashCode();
        }

        @Override
        public String toString() {
            return type.getTypeName();
        }
    }

    static class Key {

        private final Type type;
        private final Task task;
        private final String qualifier;

        Key(Type type, Task task, String qualifier) {
            this.type = Objects.requireNonNull(type, "Supplement type must not be null");
            this.task = task;
            this.qualifier = qualifier;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (object == null || getClass() != object.getClass()) {
                return false;
            }
            Key key = (Key) object;
            if (!type.equals(key.type)) {
                return false;
            }
            if (!Objects.equals(task, key.task)) {
                return false;
            }
            return Objects.equals(qualifier, key.qualifier);
        }

        @Override
        public int hashCode() {
            int result = type.hashCode();
            result = 31 * result + (task != null ? task.hashCode() : 0);
            result = 31 * result + (qualifier != null ? qualifier.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return type.getTypeName()
                + (task == null ? "" : ("@" + task))
                + (qualifier == null ? "" : ("/" + qualifier));
        }
    }
}
