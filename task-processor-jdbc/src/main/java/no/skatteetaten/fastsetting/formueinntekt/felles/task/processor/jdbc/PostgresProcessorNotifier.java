package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.jdbc;

import java.io.EOFException;
import java.net.SocketException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.ProcessorNotifier;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.TaskProcessor;
import org.postgresql.PGNotification;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.util.PSQLException;

public class PostgresProcessorNotifier implements ProcessorNotifier {

    private final UnpooledConnectionSource<PgConnection> connectionSource;

    private final Runnable onDisconnected;

    private final Executor executor;

    private final Consumer<String> callback;

    private PgConnection connection;

    public PostgresProcessorNotifier(
        UnpooledConnectionSource<PgConnection> connectionSource,
        Runnable onDisconnected,
        Executor executor,
        Collection<? extends TaskProcessor> processors,
        BiConsumer<TaskProcessor, TaskChangeEvent> callback
    ) {
        this.connectionSource = connectionSource;
        this.onDisconnected = onDisconnected;
        this.executor = executor;
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
            PgConnection conn = connectionSource.get();
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("LISTEN " + TaskChangeEvent.POSTGRES_CHANNEL);
            } catch (Throwable t) {
                try {
                    conn.close();
                } catch (Throwable suppressed) {
                    t.addSuppressed(suppressed);
                }
                throw t;
            }
            connection = conn;
            executor.execute(() -> {
                try {
                    while (!Thread.interrupted() && !conn.isClosed()) {
                        PGNotification[] notifications = conn.getNotifications(0);
                        if (notifications != null) {
                            for (PGNotification notification : notifications) {
                                callback.accept(notification.getParameter());
                            }
                        }
                    }
                } catch (PSQLException e) {
                    if (!(e.getCause() instanceof EOFException) && !(e.getCause() instanceof SocketException)) {
                        throw new IllegalStateException(e);
                    }
                } catch (SQLException e) {
                    throw new IllegalStateException(e);
                } finally {
                    try {
                        conn.close();
                    } catch (Throwable ignored) {
                        /* do nothing */
                    } finally {
                        onDisconnected.run();
                    }
                }
            });
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
}
