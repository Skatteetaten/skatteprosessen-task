package no.skatteetaten.fastsetting.formueinntekt.felles.task.jdbc;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.Task;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskCreation;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskState;

public class OracleTaskRepository extends JdbcTaskRepository {

    private static final int NOT_REGISTERED = 2291;

    private final int lockSize, bulkSize;

    private final boolean hint;

    public OracleTaskRepository(boolean concurrent, String owner) {
        super(concurrent, owner);
        lockSize = 25;
        bulkSize = 1_000;
        hint = true;
    }

    public OracleTaskRepository(boolean concurrent, String owner, int lockSize, int bulkSize, boolean hint) {
        super(concurrent, owner);
        this.lockSize = lockSize;
        this.bulkSize = bulkSize;
        this.hint = hint;
    }

    @Override
    public void heartbeat(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
            "UPDATE TASK_OWNER SET HEARTBEAT = SYSDATE WHERE OWNER = ?"
        )) {
            ps.setString(1, owner);
            if (ps.executeUpdate() == 0) {
                throw new IllegalStateException("Could not send heartbeat");
            }
        }
    }

    @Override
    public void expire(Connection conn, long timeout, TimeUnit unit) throws SQLException {
        if (concurrent) {
            try (CallableStatement ps = conn.prepareCall(
                "BEGIN "
                    + "FOR RECORD IN ("
                    + "SELECT ROWID "
                    + "FROM TASK "
                    + "WHERE STATE = ? "
                    + "AND OWNER IN ("
                    + "SELECT OWNER "
                    + "FROM TASK_OWNER "
                    + "WHERE HEARTBEAT < CURRENT_TIMESTAMP - NUMTODSINTERVAL(?, 'SECOND') "
                    + "AND OWNER != ?"
                    + ") "
                    + "FOR UPDATE SKIP LOCKED"
                    + ") "
                    + "LOOP "
                    + "UPDATE TASK SET STATE = ? WHERE ROWID = RECORD.ROWID; "
                    + "END LOOP; "
                    + "END;"
            )) {
                ps.setInt(1, TaskState.ACTIVE.ordinal());
                ps.setLong(2, unit.toSeconds(timeout));
                ps.setString(3, owner);
                ps.setInt(4, TaskState.EXPIRED.ordinal());
                ps.execute();
            }
        } else {
            try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE TASK SET STATE = ? "
                    + "WHERE STATE = ? "
                    + "AND OWNER IN ("
                    + "SELECT OWNER "
                    + "FROM TASK_OWNER "
                    + "WHERE HEARTBEAT < CURRENT_TIMESTAMP - NUMTODSINTERVAL(?, 'SECOND') "
                    + "AND OWNER != ?"
                    + ")"
            )) {
                ps.setInt(1, TaskState.EXPIRED.ordinal());
                ps.setInt(2, TaskState.ACTIVE.ordinal());
                ps.setLong(3, unit.toSeconds(timeout));
                ps.setString(4, owner);
                ps.executeUpdate();
            }
        }
    }

    @Override
    public boolean initialize(Connection conn, String topic) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
            "MERGE INTO TASK_TOPIC "
                + "USING DUAL "
                + "ON (TOPIC = ?) "
                + "WHEN NOT MATCHED THEN INSERT (TOPIC) VALUES (?)"
        )) {
            ps.setString(1, topic);
            ps.setString(2, topic);
            return ps.executeUpdate() == 1;
        }
    }

    @Override
    public void refresh(Connection conn) throws SQLException {
        try (CallableStatement cs = conn.prepareCall(
            "BEGIN "
                + "DBMS_STATS.GATHER_TABLE_STATS(USER, 'TASK'); "
                + "END;"
        )) {
            if (cs.execute()) {
                throw new IllegalStateException("Unexpected result for table refresh");
            }
        }
    }

    @Override
    public List<Task> push(Connection conn, String topic, Insertion insertion, Collection<TaskCreation> creations) throws SQLException {
        try {
            return super.push(conn, topic, insertion, creations);
        } catch (SQLException e) {
            if (e.getErrorCode() == NOT_REGISTERED) {
                throw new IllegalArgumentException("Topic " + topic + " is not registered - can only create tasks for known topics", e);
            }
            throw e;
        }
    }

    @Override
    void doFilter(Connection conn, String topic, Collection<String> identifiers, long sequence) throws SQLException {
        Set<String> checked = new HashSet<>();
        if (concurrent) {
            try (CallableStatement cs = conn.prepareCall(
                "DECLARE "
                    + "TYPE SEQUENCES IS TABLE OF TASK.SEQUENCE%TYPE INDEX BY PLS_INTEGER; "
                    + "REDUNDANT SEQUENCES; "
                    + "BEGIN "
                    + "SELECT" + (hint ? " /*+ INDEX (TASK TASK_BY_ID_IDX) */ " : " ") + "SEQUENCE "
                    + "BULK COLLECT INTO REDUNDANT "
                    + "FROM TASK "
                    + "WHERE TOPIC = ? "
                    + "AND SEQUENCE < ? "
                    + "AND IDENTIFIER IN (" + String.join(", ", Collections.nCopies(identifiers.size() == 1 ? 1 : bulkSize, "?")) + ") "
                    + "AND STATE BETWEEN ? AND ? "
                    + "FOR UPDATE SKIP LOCKED; "
                    + "FORALL IDX IN 1 .. REDUNDANT.COUNT "
                    + "UPDATE TASK SET STATE = ?, DESCENT = GREATEST(STATE, DESCENT) WHERE TOPIC = ? AND SEQUENCE = REDUNDANT(IDX); "
                    + "END;"
            )) {
                cs.setString(1, topic);
                cs.setLong(2, sequence);
                Iterator<String> it = identifiers.iterator();
                int index = 0;
                do {
                    String identifier = it.next();
                    if (checked.add(identifier)) {
                        cs.setString(++index + 2, identifier);
                    }
                    if (!it.hasNext()) {
                        while (index < bulkSize && identifiers.size() != 1) {
                            cs.setNull(++index + 2, Types.VARCHAR);
                        }
                    }
                    if (index == bulkSize || identifiers.size() == 1) {
                        cs.setInt(index + 3, TaskState.READY.ordinal());
                        cs.setInt(index + 4, TaskState.FAILED.ordinal());
                        cs.setInt(index + 5, TaskState.REDUNDANT.ordinal());
                        cs.setString(index + 6, topic);
                        cs.addBatch();
                        if (it.hasNext()) {
                            cs.setString(1, topic);
                            cs.setLong(2, sequence);
                            index = 0;
                        }
                    }
                } while (it.hasNext());
                cs.executeBatch();
            }
        } else {
            try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE" + (hint ? " /*+ INDEX (TASK TASK_BY_ID_IDX) */ " : " ") + "TASK "
                    + "SET STATE = ?, DESCENT = GREATEST(STATE, DESCENT) "
                    + "WHERE TOPIC = ? "
                    + "AND IDENTIFIER = ? "
                    + "AND SEQUENCE < ? "
                    + "AND STATE BETWEEN ? AND ? "
            )) {
                for (String identifier : identifiers) {
                    if (checked.add(identifier)) {
                        ps.setInt(1, TaskState.REDUNDANT.ordinal());
                        ps.setString(2, topic);
                        ps.setString(3, identifier);
                        ps.setLong(4, sequence);
                        ps.setInt(5, TaskState.READY.ordinal());
                        ps.setInt(6, TaskState.FAILED.ordinal());
                        ps.addBatch();
                    }
                }
                ps.executeBatch();
            }
        }
    }

    @Override
    Set<Task> doPoll(
        Connection conn, String topic, TaskState state, long from, long to, int size
    ) throws SQLException {
        if (!concurrent) {
            return super.doPoll(conn, topic, state, from, to, size);
        } else if (size == 0) {
            return Collections.emptySet();
        }
        Set<Task> tasks = new LinkedHashSet<>();
        try (PreparedStatement ps = conn.prepareStatement(
            "SELECT /*+ FIRST_ROWS_1 */ SEQUENCE, IDENTIFIER, INPUT "
                + "FROM TASK "
                + "WHERE TOPIC = ? "
                + "AND STATE = ? "
                + (from > 0 ? "AND SEQUENCE > ? " : "")
                + "ORDER BY SEQUENCE "
                + "FOR UPDATE SKIP LOCKED"
        )) {
            ps.setString(1, topic);
            ps.setInt(2, state.ordinal());
            if (from > 0) {
                ps.setLong(3, from);
            }
            ps.setFetchSize(lockSize);
            try (ResultSet rs = ps.executeQuery()) {
                while (size-- > 0 && rs.next()) {
                    tasks.add(new Task(
                        rs.getLong("SEQUENCE"),
                        rs.getString("IDENTIFIER"),
                        rs.getString("INPUT")
                    ));
                }
            }
        }
        return tasks;
    }

    @Override
    public void purgeAll(Connection conn, String topic) throws SQLException {
        boolean purged;
        try (CallableStatement cs = conn.prepareCall(
            "CALL TASK_TOPIC_PURGE_ALL(?) INTO ?"
        )) {
            cs.setString(1, topic);
            cs.registerOutParameter(2, Types.INTEGER);
            purged = !cs.execute() && cs.getBoolean(2);
        }
        if (!purged) {
            super.purgeAll(conn, topic);
        }
    }

    @Override
    long doPurge(Connection conn, String topic, long from, long to, TaskState lower, TaskState upper) throws SQLException {
        if (concurrent) {
            try (CallableStatement cs = conn.prepareCall(
                "DECLARE "
                    + "TYPE SEQUENCES IS TABLE OF TASK.SEQUENCE%TYPE INDEX BY PLS_INTEGER; "
                    + "PURGED SEQUENCES; "
                    + "BEGIN "
                    + "SELECT" + (hint ? " /*+ INDEX (TASK TASK_BY_TOPIC_IDX) */ " : " ") + "SEQUENCE "
                    + "BULK COLLECT INTO PURGED "
                    + "FROM TASK "
                    + "WHERE TOPIC = ? "
                    + (from == INCEPTION && to == Long.MAX_VALUE ? "" : "AND SEQUENCE < ? ")
                    + "AND STATE BETWEEN ? AND ? "
                    + "FOR UPDATE SKIP LOCKED; "
                    + "FORALL IDX IN 1 .. PURGED.COUNT "
                    + "DELETE FROM TASK WHERE TOPIC = ? AND SEQUENCE = PURGED(IDX); "
                    + "? := SQL%ROWCOUNT; "
                    + "END;"
            )) {
                int index = 0;
                cs.setString(++index, topic);
                if (from != INCEPTION || to != Long.MAX_VALUE) {
                    cs.setLong(++index, from);
                    cs.setLong(++index, to);
                }
                cs.setInt(++index, lower.ordinal());
                cs.setInt(++index, upper.ordinal());
                cs.setString(++index, topic);
                cs.registerOutParameter(++index, Types.INTEGER);
                cs.execute();
                return cs.getLong(index);
            }
        } else {
            return super.doPurge(conn, topic, from, to, lower, upper);
        }
    }

    @Override
    String getCountingHint(boolean identified) {
        return hint ? " /*+ PARALLEL_INDEX(TASK "
            + (identified ? "TASK_BY_ID_IDX" : "TASK_BY_TOPIC_IDX")
            + " 35) PARALLEL(TASK 35) */ " : super.getCountingHint(identified);
    }
}
