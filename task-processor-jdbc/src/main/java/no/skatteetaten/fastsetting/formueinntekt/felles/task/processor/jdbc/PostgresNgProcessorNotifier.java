package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import com.impossibl.postgres.api.jdbc.PGConnection;
import com.impossibl.postgres.api.jdbc.PGNotificationListener;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.ProcessorNotifier;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.TaskProcessor;

public class PostgresNgProcessorNotifier implements ProcessorNotifier, PGNotificationListener {

    private final UnpooledConnectionSource<PGConnection> connectionSource;

    private final Runnable onDisconnect;

    private final Consumer<String> callback;

    private Connection connection;

    public PostgresNgProcessorNotifier(
        UnpooledConnectionSource<PGConnection> connectionSource,
        Runnable onDisconnect,
        Collection<? extends TaskProcessor> processors,
        BiConsumer<TaskProcessor, TaskChangeEvent> callback
    ) {
        this.connectionSource = connectionSource;
        this.onDisconnect = onDisconnect;
        this.callback = TaskChangeEvent.postgres(processors, callback);
    }

    @Override
    public synchronized boolean isActive() {
        try {
            return connection != null && !connection.isClosed();
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public synchronized boolean start() {
        try {
            if (connection != null && !connection.isClosed()) {
                return false;
            }
            PGConnection conn = connectionSource.get();
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("LISTEN " + TaskChangeEvent.POSTGRES_CHANNEL);
            }
            conn.addNotificationListener(toString(), TaskChangeEvent.POSTGRES_CHANNEL, this);
            connection = conn;
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
        return true;
    }

    @Override
    public synchronized boolean stop() {
        try {
            if (connection == null || connection.isClosed()) {
                return false;
            }
            connection.close();
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
        return true;
    }

    @Override
    public void notification(int process, String channel, String payload) {
        callback.accept(payload);
    }

    @Override
    public void closed() {
        onDisconnect.run();
    }
}
