package no.skatteetaten.fastsetting.formueinntekt.felles.task.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskRepository;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

@Category(PostgreSQLContainer.class)
@RunWith(Parameterized.class)
public class PostgresTaskRepositoryTest extends AbstractTaskRepositoryTest<HikariDataSource> {

    @Parameterized.Parameters(name = "concurrent: {0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {true},
            {false}
        });
    }

    @Rule
    public JdbcDatabaseContainer<?> container = new PostgreSQLContainer<>("postgres:11");

    private final boolean concurrent;

    public PostgresTaskRepositoryTest(boolean concurrent) {
        super(TaskRepository.INCEPTION);
        this.concurrent = concurrent;
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
    protected TaskRepository<Connection, SQLException> getRepository(String owner) {
        return new PostgresTaskRepository(concurrent, owner);
    }
}
