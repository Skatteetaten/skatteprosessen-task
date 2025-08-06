package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.sql.DataSource;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.TaskProcessor;

public class OraclePersistentTaskProcessor extends JdbcPersistentTaskProcessor {

    public OraclePersistentTaskProcessor(TaskProcessor delegate, DataSource dataSource) {
        super(delegate, dataSource);
    }

    @Override
    protected void write(Activation activation) {
        String topic = getTopic();
        try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(
            "MERGE INTO TASK_ACTIVATION "
                + "USING DUAL "
                + "ON (TOPIC = ?) "
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
