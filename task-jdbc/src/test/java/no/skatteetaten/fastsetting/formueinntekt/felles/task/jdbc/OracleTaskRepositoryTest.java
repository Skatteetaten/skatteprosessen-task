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
import org.testcontainers.containers.OracleContainer;

@Category(OracleContainer.class)
@RunWith(Parameterized.class)
public class OracleTaskRepositoryTest extends AbstractTaskRepositoryTest<HikariDataSource> {

    @Parameterized.Parameters(name = "concurrent: {0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
            {true},
            {false}
        });
    }

    @Rule
    public JdbcDatabaseContainer<?> container = new OracleContainer("gvenzl/oracle-xe");

    private final boolean concurrent;

    public OracleTaskRepositoryTest(boolean concurrent) {
        // Strangely, Oracle increments the sequence by 1 upon defining a partition.
        super(TaskRepository.INCEPTION + 1);
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
        return new OracleTaskRepository(concurrent, owner);
    }
}
