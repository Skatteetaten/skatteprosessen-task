package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.sql.DataSource;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.TaskProcessor;

public class PostgresPersistentTaskProcessor extends JdbcPersistentTaskProcessor {

    public PostgresPersistentTaskProcessor(TaskProcessor delegate, DataSource dataSource) {
        super(delegate, dataSource);
    }

    @Override
    protected void write(Activation activation) {
        String topic = getTopic();
        try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(
            "INSERT INTO TASK_ACTIVATION (TOPIC, ACTIVE) VALUES (?, ?) "
                + "ON CONFLICT (TOPIC) "
                + "DO UPDATE SET ACTIVE = ?"
        )) {
            ps.setString(1, topic);
            ps.setBoolean(2, activation == Activation.ACTIVE);
            ps.setBoolean(3, activation == Activation.ACTIVE);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }
}
