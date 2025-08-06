package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.jdbc;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.jdbc.JdbcTaskRepository;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.jdbc.OracleTaskRepository;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.ProcessorNotifier;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.TaskProcessor;
import oracle.jdbc.pool.OracleDataSource;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.OracleContainer;

@Category(OracleContainer.class)
public class OracleProcessorNotifierTest extends AbstractJdbcProcessorNotifierTest<HikariDataSource> {

    @Rule
    public JdbcDatabaseContainer<?> container = new OracleContainer("gvenzl/oracle-xe").withAccessToHost(true);

    private final int port;

    public OracleProcessorNotifierTest() {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            port = socket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        Testcontainers.exposeHostPorts(port);
    }

    @Override
    protected HikariDataSource dataSource() throws SQLException {
        OracleDataSource dataSource = new OracleDataSource();
        dataSource.setURL(container.getJdbcUrl());
        dataSource.setUser(container.getUsername());
        dataSource.setPassword(container.getPassword());
        try (Connection conn = dataSource.getConnection("system", container.getPassword()); Statement stmt = conn.createStatement()) {
            stmt.execute("GRANT CHANGE NOTIFICATION TO " + container.getUsername());
        }
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(container.getJdbcUrl());
        config.setUsername(container.getUsername());
        config.setPassword(container.getPassword());
        return new HikariDataSource(config);
    }

    @Override
    protected void shutdown(HikariDataSource dataSource) {
        dataSource.close();
    }

    @Override
    protected ProcessorNotifier processorNotifier(HikariDataSource dataSource, Collection<TaskProcessor> processors) {
        return new OracleProcessorNotifier(dataSource, processors, new DefaultTaskChangeHandler(1, TimeUnit.SECONDS), GenericContainer.INTERNAL_HOST_HOSTNAME, port);
    }

    @Override
    protected JdbcTaskRepository taskRepository(String owner) {
        return new OracleTaskRepository(false, owner);
    }
}
