package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.jdbc;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import com.impossibl.postgres.api.jdbc.PGConnection;
import com.impossibl.postgres.jdbc.PGDataSource;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.jdbc.JdbcTaskRepository;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.jdbc.PostgresTaskRepository;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.ProcessorNotifier;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.TaskProcessor;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

@Category(PostgreSQLContainer.class)
public class PostgresNgProcessorNotifierTest extends AbstractJdbcProcessorNotifierTest<PGDataSource> {

    @Rule
    public JdbcDatabaseContainer<?> container = new PostgreSQLContainer<>("postgres:11");

    @Override
    protected PGDataSource dataSource() {
        PGDataSource dataSource = new PGDataSource();
        dataSource.setHost(container.getHost());
        dataSource.setUser(container.getUsername());
        dataSource.setPassword(container.getPassword());
        dataSource.setPort(container.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT));
        dataSource.setDatabaseName(container.getDatabaseName());
        return dataSource;
    }

    @Override
    protected void shutdown(PGDataSource dataSource) { }

    @Override
    protected ProcessorNotifier processorNotifier(PGDataSource dataSource, Collection<TaskProcessor> processors) {
        return new PostgresNgProcessorNotifier(
            () -> dataSource.getConnection().unwrap(PGConnection.class),
            () -> { },
            processors,
            new DefaultTaskChangeHandler(1, TimeUnit.SECONDS)
        );
    }

    @Override
    protected JdbcTaskRepository taskRepository(String owner) {
        return new PostgresTaskRepository(false, owner);
    }
}
