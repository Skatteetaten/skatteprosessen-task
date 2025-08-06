package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.jdbc;

import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;

import no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.TaskProcessor;
import org.hsqldb.jdbc.JDBCPool;

public class HsqldbTaskProcessorTest extends AbstractJdbcPersistentTaskProcessorTest<JDBCPool> {

    @Override
    protected JDBCPool dataSource() {
        JDBCPool dataSource = new JDBCPool();
        dataSource.setUrl("jdbc:hsqldb:mem:feed" + ThreadLocalRandom.current().nextLong());
        dataSource.setUser("sa");
        dataSource.setPassword("");
        return dataSource;
    }

    @Override
    protected void shutdown(JDBCPool dataSource) throws SQLException {
        dataSource.close(0);
    }

    @Override
    protected JdbcPersistentTaskProcessor processor(TaskProcessor delegate, JDBCPool dataSource) {
        return new JdbcPersistentTaskProcessor(delegate, dataSource);
    }
}
