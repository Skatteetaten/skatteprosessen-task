package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.sql.DataSource;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.TaskProcessor;

public class JdbcPersistentTaskProcessor implements TaskProcessor {

    public static final String CHANGE_LOG = "liquibase/taskAccessChangeLog.xml";

    private final TaskProcessor delegate;
    protected final DataSource dataSource;

    public JdbcPersistentTaskProcessor(TaskProcessor delegate, DataSource dataSource) {
        this.delegate = delegate;
        this.dataSource = dataSource;
    }

    @Override
    public synchronized boolean start(long timeout, TimeUnit timeUnit) throws TimeoutException, InterruptedException {
        if (delegate.start(timeout, timeUnit)) {
            write(Activation.ACTIVE);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public synchronized boolean stop(long timeout, TimeUnit timeUnit) throws TimeoutException, InterruptedException {
        if (delegate.stop(timeout, timeUnit)) {
            write(Activation.INACTIVE);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean resume() {
        return delegate.resume();
    }

    @Override
    public boolean isActive() {
        return delegate.isActive();
    }

    @Override
    public String getTopic() {
        return delegate.getTopic();
    }

    @Override
    public Optional<Activation> findActivation() {
        try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(
            "SELECT ACTIVE "
                + "FROM TASK_ACTIVATION "
                + "WHERE TOPIC = ?")
        ) {
            ps.setString(1, getTopic());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(rs.getBoolean("ACTIVE") ? Activation.ACTIVE : Activation.INACTIVE);
                } else {
                    return Optional.empty();
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public boolean initialize(long timeout, TimeUnit timeUnit) throws TimeoutException, InterruptedException {
        Optional<Activation> activation = findActivation();
        if (activation.isPresent()) {
            if (activation.get() == Activation.ACTIVE) {
                return delegate.initialize(timeout, timeUnit);
            }
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean shutdown(long timeout, TimeUnit timeUnit) throws TimeoutException, InterruptedException {
        return delegate.shutdown(timeout, timeUnit);
    }

    protected void write(Activation activation) {
        String topic = getTopic();
        try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(
            "MERGE INTO TASK_ACTIVATION "
                + "USING (VALUES ?) AS TARGET (TOPIC) "
                + "ON TASK_ACTIVATION.TOPIC = TARGET.TOPIC "
                + "WHEN MATCHED THEN UPDATE SET ACTIVE = ? "
                + "WHEN NOT MATCHED THEN INSERT (TOPIC, ACTIVE) VALUES (?, ?)"
        )) {
            ps.setString(1, topic);
            ps.setBoolean(2, activation == Activation.ACTIVE);
            ps.setString(3, topic);
            ps.setBoolean(4, activation == Activation.ACTIVE);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }
}
