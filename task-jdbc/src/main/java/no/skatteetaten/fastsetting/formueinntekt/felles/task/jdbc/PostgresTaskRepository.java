package no.skatteetaten.fastsetting.formueinntekt.felles.task.jdbc;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.Task;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskCreation;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskState;

public class PostgresTaskRepository extends JdbcTaskRepository.WithIntervalMultiplication {

    private final int bulkSize;

    public PostgresTaskRepository(boolean concurrent, String owner) {
        super(concurrent, owner);
        bulkSize = 1_000;
    }

    public PostgresTaskRepository(boolean concurrent, String owner, int bulkSize) {
        super(concurrent, owner);
        this.bulkSize = bulkSize;
    }

    @Override
    public boolean initialize(Connection conn, String topic) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
            "INSERT INTO TASK_TOPIC (TOPIC) VALUES (?) ON CONFLICT DO NOTHING"
        )) {
            ps.setString(1, topic);
            return ps.executeUpdate() == 1;
        }
    }

    @Override
    public void refresh(Connection conn) throws SQLException {
        try (CallableStatement cs = conn.prepareCall(
            "ANALYZE TASK"
        )) {
            if (cs.execute()) {
                throw new IllegalStateException("Unexpected result for table refresh");
            }
        }
    }

    @Override
    public List<Task> push(
        Connection conn, String topic, Insertion insertion, Collection<TaskCreation> creations
    ) throws SQLException {
        if (creations.isEmpty()) {
            return Collections.emptyList();
        }
        List<Task> tasks = new ArrayList<>(creations.size());
        Map<String, Long> duplicates = insertion == Insertion.REPLACE
            ? creations.stream().collect(Collectors.groupingBy(TaskCreation::getIdentifier, Collectors.counting()))
            : null;
        List<TaskCreation> remaining = new ArrayList<>(creations);
        do {
            int bulkSize = (int) Math.pow(2, (int) (Math.log(remaining.size()) / Math.log(2)));
            do {
                try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO TASK (TOPIC, IDENTIFIER" + (duplicates == null ? "" : ", STATE") + ", INPUT) "
                        + "VALUES " + String.join(", ", Collections.nCopies(bulkSize, "(?, ?, ?" + (duplicates == null ? "" : ", ?") + ")")) + " "
                        + "RETURNING SEQUENCE"
                )) {
                    for (int index = 0; index < bulkSize; index++) {
                        if (duplicates == null) {
                            ps.setString(3 * index + 1, topic);
                            ps.setString(3 * index + 2, remaining.get(index).getIdentifier());
                            ps.setString(3 * index + 3, remaining.get(index).getInput().orElse(null));
                        } else {
                            ps.setString(4 * index + 1, topic);
                            ps.setString(4 * index + 2, remaining.get(index).getIdentifier());
                            ps.setInt(4 * index + 3, (duplicates.computeIfPresent(remaining.get(index).getIdentifier(), (ignored, count) -> --count) == 0
                                ? TaskState.READY
                                : TaskState.REDUNDANT).ordinal());
                            ps.setString(4 * index + 4, remaining.get(index).getInput().orElse(null));
                        }
                    }
                    try (ResultSet rs = ps.executeQuery()) {
                        for (int index = 0; index < bulkSize; index++) {
                            if (!rs.next()) {
                                throw new IllegalStateException("Expected generated key for task");
                            }
                            tasks.add(new Task(
                                rs.getLong("SEQUENCE"),
                                remaining.get(index).getIdentifier(),
                                remaining.get(index).getInput().orElse(null)
                            ));
                        }
                        if (rs.next()) {
                            throw new IllegalStateException("Unexpected generated key");
                        }
                    } catch (SQLException e) {
                        if (e.getMessage() != null && e.getMessage().startsWith("ERROR: no partition")) {
                            throw new IllegalArgumentException("Topic " + topic + " is not registered - cannot create tasks without handler", e);
                        }
                        throw e;
                    }
                }
                remaining = remaining.subList(bulkSize, remaining.size());
            } while (remaining.size() >= bulkSize);
        } while (!remaining.isEmpty());
        if (insertion == Insertion.REPLACE) {
            doFilter(conn, topic, creations.stream().map(TaskCreation::getIdentifier).collect(Collectors.toSet()), tasks.get(0).getSequence());
        }
        return tasks;
    }

    @Override
    void doFilter(Connection conn, String topic, Collection<String> identifiers, long sequence) throws SQLException {
        Set<String> checked = new HashSet<>();
        try (PreparedStatement ps = conn.prepareStatement(
            "UPDATE TASK "
                + "SET STATE = ?, DESCENT = GREATEST(STATE, DESCENT) "
                + "WHERE TOPIC = ? "
                + (concurrent ? "AND SEQUENCE IN (SELECT SEQUENCE FROM TASK WHERE TOPIC = ? " : "")
                + "AND SEQUENCE < ? "
                + "AND IDENTIFIER " + (identifiers.size() == 1 ? "= ?" : ("IN (" + String.join(", ", Collections.nCopies(bulkSize, "?")) + ")")) + " "
                + "AND STATE BETWEEN ? AND ?"
                + (concurrent ? " FOR UPDATE SKIP LOCKED)" : "")
        )) {
            ps.setInt(1, TaskState.REDUNDANT.ordinal());
            ps.setString(2, topic);
            if (concurrent) {
                ps.setString(3, topic);
            }
            ps.setLong(3 + (concurrent ? 1 : 0), sequence);
            Iterator<String> it = identifiers.iterator();
            int index = 0;
            do {
                String identifier = it.next();
                if (checked.add(identifier)) {
                    ps.setString(++index + 3 + (concurrent ? 1 : 0), identifier);
                }
                if (!it.hasNext()) {
                    while (index < bulkSize && identifiers.size() != 1) {
                        ps.setNull(++index + 3 + (concurrent ? 1 : 0), Types.VARCHAR);
                    }
                }
                if (index == bulkSize || identifiers.size() == 1) {
                    ps.setInt(index + 4 + (concurrent ? 1 : 0), TaskState.READY.ordinal());
                    ps.setInt(index + 5 + (concurrent ? 1 : 0), TaskState.FAILED.ordinal());
                    ps.addBatch();
                    if (it.hasNext()) {
                        ps.setInt(1, TaskState.REDUNDANT.ordinal());
                        ps.setString(2, topic);
                        if (concurrent) {
                            ps.setString(3, topic);
                        }
                        ps.setLong(3 + (concurrent ? 1 : 0), sequence);
                        index = 0;
                    }
                }
            } while (it.hasNext());
            ps.executeBatch();
        }
    }

    @Override
    public void purgeAll(Connection conn, String topic) throws SQLException {
        boolean purged;
        try (CallableStatement cs = conn.prepareCall(
            "{? = CALL TASK_TOPIC_PURGE_ALL(?)}"
        )) {
            cs.registerOutParameter(1, Types.BOOLEAN);
            cs.setString(2, topic);
            purged = !cs.execute() && cs.getBoolean(1);
        } catch (SQLException e) {
            if (e.getMessage().startsWith("ERROR: relation \"")) {
                purged = true;
            } else {
                throw e;
            }
        }
        if (!purged) {
            super.purgeAll(conn, topic);
        }
    }

    @Override
    long doPurge(Connection conn, String topic, long from, long to, TaskState lower, TaskState upper) throws SQLException {
        if (concurrent) {
            try (PreparedStatement ps = conn.prepareStatement(
                "DELETE FROM TASK "
                    + "WHERE TOPIC = ? "
                    + "AND SEQUENCE IN (SELECT SEQUENCE FROM TASK WHERE TOPIC = ? "
                    + (from == INCEPTION && to == Long.MAX_VALUE ? "" : "AND SEQUENCE BETWEEN ? AND ? ")
                    + "AND STATE BETWEEN ? AND ? "
                    + "FOR UPDATE SKIP LOCKED)"
            )) {
                ps.setString(1, topic);
                ps.setString(2, topic);
                if (from != INCEPTION || to != Long.MAX_VALUE) {
                    ps.setLong(3, from);
                    ps.setLong(4, to);
                    ps.setInt(5, lower.ordinal());
                    ps.setInt(6, upper.ordinal());
                } else {
                    ps.setInt(3, lower.ordinal());
                    ps.setInt(4, upper.ordinal());
                }
                return ps.executeLargeUpdate();
            }
        } else {
            return super.doPurge(conn, topic, from, to, lower, upper);
        }
    }
}
