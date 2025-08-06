package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.function.BiConsumer;

import javax.sql.DataSource;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskState;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.ProcessorNotifier;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.TaskProcessor;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.OraclePreparedStatement;
import oracle.jdbc.datasource.OracleDataSource;
import oracle.jdbc.dcn.DatabaseChangeRegistration;
import oracle.jdbc.dcn.TableChangeDescription;

public class OracleProcessorNotifier implements ProcessorNotifier {

    private final DataSource dataSource;

    private final Collection<? extends TaskProcessor> processors;

    private final BiConsumer<TaskProcessor, TaskChangeEvent> callback;

    private final String host;

    private final int port;

    private List<DatabaseChangeRegistration> registrations;

    public OracleProcessorNotifier(
        DataSource dataSource,
        Collection<? extends TaskProcessor> processors,
        BiConsumer<TaskProcessor, TaskChangeEvent> callback
    ) {
        this.dataSource = checked(dataSource);
        this.processors = processors;
        this.callback = callback;
        host = null;
        port = 0;
    }

    public OracleProcessorNotifier(
        DataSource dataSource,
        Collection<? extends TaskProcessor> processors,
        BiConsumer<TaskProcessor, TaskChangeEvent> callback,
        String host, int port
    ) {
        this.dataSource = checked(dataSource);
        this.processors = processors;
        this.callback = callback;
        this.host = host;
        this.port = port;
    }

    private static DataSource checked(DataSource dataSource) {
        try {
            if (!dataSource.isWrapperFor(OracleDataSource.class)) {
                throw new IllegalArgumentException("Not a Oracle datasource: " + dataSource);
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
        return dataSource;
    }

    @Override
    public synchronized boolean isActive() {
        return registrations != null;
    }

    @Override
    public synchronized boolean start() {
        if (registrations != null) {
            return false;
        }
        Properties properties = new Properties();
        properties.setProperty(OracleConnection.DCN_IGNORE_INSERTOP, "false");
        properties.setProperty(OracleConnection.DCN_IGNORE_UPDATEOP, "false");
        properties.setProperty(OracleConnection.DCN_IGNORE_DELETEOP, "true");
        properties.setProperty(OracleConnection.DCN_QUERY_CHANGE_NOTIFICATION, "true");
        if (host != null) {
            properties.setProperty(OracleConnection.NTF_LOCAL_HOST, host);
        }
        if (port > 0) {
            properties.setProperty(OracleConnection.NTF_LOCAL_TCP_PORT, Integer.toString(port));
        }
        List<DatabaseChangeRegistration> registrations = new ArrayList<>();
        try (Connection conn = dataSource.getConnection()) {
            for (TaskProcessor processor : processors) {
                DatabaseChangeRegistration registration = conn.unwrap(OracleConnection.class).registerDatabaseChangeNotification(properties);
                registrations.add(registration);
                registration.addListener(event -> {
                    boolean task = false, activation = false;
                    for (TableChangeDescription description : event.getTableChangeDescription()) {
                        switch (description.getTableName()) {
                        case "TASK":
                            task = true;
                            break;
                        case "TASK_ACTIVATION":
                            activation = true;
                            break;
                        }
                    }
                    if (task) {
                        callback.accept(processor, TaskChangeEvent.WORK);
                    }
                    if (activation) {
                        callback.accept(processor, TaskChangeEvent.ACTIVATION);
                    }
                });
                try (OraclePreparedStatement ps = conn.prepareStatement(
                    "SELECT SEQUENCE FROM TASK WHERE TOPIC = ? AND STATE = ?"
                ).unwrap(OraclePreparedStatement.class)) {
                    ps.setDatabaseChangeRegistration(registration);
                    ps.setString(1, processor.getTopic());
                    ps.setInt(2, TaskState.ACTIVE.ordinal());
                    ps.executeQuery().close();
                }
                try (OraclePreparedStatement ps = conn.prepareStatement(
                    "SELECT ACTIVE FROM TASK_ACTIVATION WHERE TOPIC = ?"
                ).unwrap(OraclePreparedStatement.class)) {
                    ps.setDatabaseChangeRegistration(registration);
                    ps.setString(1, processor.getTopic());
                    ps.executeQuery().close();
                }
            }
        } catch (Exception e) {
            try {
                doStop(registrations);
            } catch (Exception suppressed) {
                e.addSuppressed(suppressed);
            }
            throw new IllegalStateException(e);
        }
        this.registrations = registrations;
        return true;
    }

    private void doStop(List<DatabaseChangeRegistration> registrations) {
        List<Exception> exceptions = new ArrayList<>();
        try (OracleConnection conn = dataSource.getConnection().unwrap(OracleConnection.class)) {
            for (DatabaseChangeRegistration registration : registrations) {
                try {
                    conn.unregisterDatabaseChangeNotification(registration);
                } catch (Exception e) {
                    exceptions.add(e);
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
        if (!exceptions.isEmpty()) {
            RuntimeException exception = new RuntimeException("Failed to unregister change notifications");
            exceptions.forEach(exception::addSuppressed);
            throw exception;
        }
    }

    @Override
    public synchronized boolean stop() {
        if (registrations == null) {
            return false;
        }
        doStop(registrations);
        registrations = null;
        return true;
    }
}
