package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.jdbc;

import java.sql.DriverManager;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.jdbc.JdbcTaskRepository;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.jdbc.PostgresTaskRepository;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.ProcessorNotifier;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.TaskProcessor;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.postgresql.jdbc.PgConnection;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

@Category(PostgreSQLContainer.class)
public class PostgresProcessorNotifierTest extends AbstractJdbcProcessorNotifierTest<HikariDataSource> {

    @Rule
    public JdbcDatabaseContainer<?> container = new PostgreSQLContainer<>("postgres:11");

    private ExecutorService executorService;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        executorService = Executors.newSingleThreadExecutor();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        executorService.shutdown();

        super.tearDown();
    }

    @Override
    protected HikariDataSource dataSource() {
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
        return new PostgresProcessorNotifier(
            () -> DriverManager.getConnection(dataSource.getJdbcUrl(), dataSource.getUsername(), dataSource.getPassword()).unwrap(PgConnection.class),
            () -> { },
            executorService,
            processors,
            new DefaultTaskChangeHandler(1, TimeUnit.SECONDS)
        );
    }

    @Override
    protected JdbcTaskRepository taskRepository(String owner) {
        return new PostgresTaskRepository(false, owner);
    }
}
