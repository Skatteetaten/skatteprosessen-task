package no.skatteetaten.fastsetting.formueinntekt.felles.task.jdbc;

import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskRepository;
import org.hsqldb.jdbc.JDBCPool;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;

public class HsqldbJdbcTaskRepositoryTest extends AbstractTaskRepositoryTest<JDBCPool> {

    public HsqldbJdbcTaskRepositoryTest() {
        super(TaskRepository.INCEPTION);
    }

    @Override
    protected JDBCPool dataSource() {
        JDBCPool dataSource = new JDBCPool();
        dataSource.setUrl("jdbc:hsqldb:mem:task:" + ThreadLocalRandom.current().nextLong());
        dataSource.setUser("sa");
        dataSource.setPassword("");

        return dataSource;
    }

    @Override
    protected void shutdown(JDBCPool dataSource) throws SQLException {
        dataSource.close(0);
    }

    @Override
    protected TaskRepository<Connection, SQLException> getRepository(String owner) {
        return new JdbcTaskRepository(false, owner);
    }
}
