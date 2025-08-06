package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.jdbc;

import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import io.vertx.pgclient.pubsub.PgChannel;
import io.vertx.pgclient.pubsub.PgSubscriber;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.ProcessorNotifier;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.TaskProcessor;

public class PostgresVertxProcessorNotifier implements ProcessorNotifier {

    private final PgSubscriber subscriber;

    private PgChannel channel;

    private final Runnable onDisconnect;

    private final Consumer<String> callback;

    private boolean paused = true;

    public PostgresVertxProcessorNotifier(
        PgSubscriber subscriber,
        Runnable onDisconnect,
        Collection<? extends TaskProcessor> processors,
        BiConsumer<TaskProcessor, TaskChangeEvent> callback
    ) {
        this.subscriber = subscriber;
        this.onDisconnect = onDisconnect;
        this.callback = TaskChangeEvent.postgres(processors, callback);
    }

    @Override
    public synchronized boolean isActive() {
        return !paused;
    }

    @Override
    public synchronized boolean start() {
        if (!paused) {
            return false;
        }
        channel = subscriber.channel(TaskChangeEvent.POSTGRES_CHANNEL).handler(callback::accept).endHandler(ignored -> {
            synchronized (this) {
                paused = true;
            }
            onDisconnect.run();
        });
        paused = false;
        return true;
    }

    @Override
    public synchronized boolean stop() {
        if (paused) {
            return false;
        }
        channel.pause();
        paused = true;
        return true;
    }
}
