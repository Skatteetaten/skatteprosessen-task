package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.jdbc;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.TaskProcessor;

public enum TaskChangeEvent {

    ACTIVATION,
    WORK;

    static final String POSTGRES_CHANNEL = "task_notification";

    static Consumer<String> postgres(Collection<? extends TaskProcessor> processors, BiConsumer<TaskProcessor, TaskChangeEvent> callback) {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
        Map<String, ? extends TaskProcessor> md5s = processors.stream().collect(Collectors.toMap(processor -> {
            byte[] bytes = digest.digest(processor.getTopic().getBytes(StandardCharsets.UTF_8));
            return String.format("%0" + (bytes.length << 1) + "x", new BigInteger(1, bytes));
        }, Function.identity()));
        return value -> {
            if (value != null) {
                String[] arguments = value.split(" ", 2);
                if (arguments.length == 2) {
                    TaskProcessor processor = md5s.get(arguments[1]);
                    if (processor != null) {
                        callback.accept(processor, TaskChangeEvent.valueOf(arguments[0]));
                    }
                }
            }
        };
    }
}
