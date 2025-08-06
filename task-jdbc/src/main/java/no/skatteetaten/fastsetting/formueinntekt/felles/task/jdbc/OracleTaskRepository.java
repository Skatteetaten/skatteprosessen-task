package no.skatteetaten.fastsetting.formueinntekt.felles.task.jdbc;

import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.Task;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskCreation;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskState;
import oracle.jdbc.OraclePreparedStatement;

import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

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
    public boolean register(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
            "MERGE INTO TASK_OWNER "
                + "USING DUAL "
                + "ON (OWNER = ?) "
                + "WHEN NOT MATCHED THEN INSERT (OWNER) VALUES (?)"
        )) {
            ps.setString(1, owner);
            ps.setString(2, owner);
            return ps.executeUpdate() == 1;
        }
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
                    + "WHERE HEARTBEAT < CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP) - NUMTODSINTERVAL(?, 'SECOND') "
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
                    + "WHERE HEARTBEAT < CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP) - NUMTODSINTERVAL(?, 'SECOND') "
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
    void doFilter(Connection conn, String topic, Collection<String> identifiers, long sequence, TaskState from, TaskState to, boolean delete) throws SQLException {
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
                    + (delete ? "DELETE FROM TASK " : "UPDATE TASK SET STATE = ?, DESCENT = GREATEST(STATE, DESCENT) ")
                    + "WHERE TOPIC = ? "
                    + "AND SEQUENCE = REDUNDANT(IDX); "
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
                        cs.setInt(index + 3, from.ordinal());
                        cs.setInt(index + 4, to.ordinal());
                        if (delete) {
                            cs.setString(index + 5, topic);
                        } else {
                            cs.setInt(index + 5, TaskState.REDUNDANT.ordinal());
                            cs.setString(index + 6, topic);
                        }
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
                (delete ? "DELETE FROM" : "UPDATE") + (hint ? " /*+ INDEX (TASK TASK_BY_ID_IDX) */ " : " ") + "TASK "
                    + (delete ? "" : "SET STATE = ?, DESCENT = GREATEST(STATE, DESCENT) ")
                    + "WHERE TOPIC = ? "
                    + "AND IDENTIFIER = ? "
                    + "AND SEQUENCE < ? "
                    + "AND STATE BETWEEN ? AND ? "
            )) {
                for (String identifier : identifiers) {
                    if (checked.add(identifier)) {
                        if (!delete) {
                            ps.setInt(1, TaskState.REDUNDANT.ordinal());
                        }
                        ps.setString(1 + (delete ? 0 : 1), topic);
                        ps.setString(2 + (delete ? 0 : 1), identifier);
                        ps.setLong(3 + (delete ? 0 : 1), sequence);
                        ps.setInt(4 + (delete ? 0 : 1), from.ordinal());
                        ps.setInt(5 + (delete ? 0 : 1), to.ordinal());
                        ps.addBatch();
                    }
                }
                ps.executeBatch();
            }
        }
    }

    @Override
    Set<Task> doPoll(
        Connection conn, String topic, TaskState state, Order order, Condition condition, long from, long to, int size
    ) throws SQLException {
        if (!concurrent) {
            return super.doPoll(conn, topic, state, order, condition, from, to, size);
        } else if (size == 0) {
            return Set.of();
        }
        Set<Task> tasks = new LinkedHashSet<>();
        try (PreparedStatement ps = conn.prepareStatement(
            "SELECT" + (hint ? " /*+ FIRST_ROWS_1 */ " : " ") + "SEQUENCE, IDENTIFIER, INPUT "
                + "FROM TASK " + (condition == Condition.NONE ? "" : "CANDIDATE ")
                + "WHERE TOPIC = ? "
                + "AND STATE = ? "
                + (condition == Condition.NONE ? "" : ("AND IDENTIFIER NOT IN ("
                + "SELECT IDENTIFIER "
                + "FROM TASK CONTROL "
                + "WHERE CONTROL.IDENTIFIER = CANDIDATE.IDENTIFIER "
                + "AND (CONTROL.SEQUENCE " + (order == Order.FIRST_IN_FIRST_OUT ? "<" : ">") + " CANDIDATE.SEQUENCE "
                + "AND STATE BETWEEN ? AND ?"
                + (condition == Condition.SINGULAR_BY_IDENTIFIER_SUSPEND_ON_FAILURE || condition == Condition.SINGULAR_BY_IDENTIFIER_SUSPEND_UNTIL_SUCCESS ? " OR STATE BETWEEN ? AND ?" : "")
                + ")) "))
                + (from == 0 && to == Long.MAX_VALUE ? "" : "AND SEQUENCE BETWEEN ? AND ? ")
                + "ORDER BY SEQUENCE " + (order == Order.LAST_IN_FIRST_OUT ? "DESC " : "ASC ")
                + "FOR UPDATE SKIP LOCKED"
        )) {
            ps.setString(1, topic);
            ps.setInt(2, state.ordinal());
            int index = 3;
            if (condition != Condition.NONE) {
                ps.setInt(index++, TaskState.ACTIVE.ordinal());
                ps.setInt(index++, TaskState.READY.ordinal());
            }
            if (condition == Condition.SINGULAR_BY_IDENTIFIER_SUSPEND_ON_FAILURE) {
                ps.setInt(index++, TaskState.EXPIRED.ordinal());
                ps.setInt(index++, TaskState.FAILED.ordinal());
            } else if (condition == Condition.SINGULAR_BY_IDENTIFIER_SUSPEND_UNTIL_SUCCESS) {
                ps.setInt(index++, TaskState.EXPIRED.ordinal());
                ps.setInt(index++, TaskState.SUSPENDED.ordinal());
            }
            if (from != 0 || to != Long.MAX_VALUE) {
                ps.setLong(index++, from);
                ps.setLong(index, to);
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
    Set<String> doJunction(Connection conn, String topic, TaskState lower, TaskState upper, Collection<Set<String>> groups) throws SQLException {
        int maximum = groups.stream().mapToInt(Set::size).max().orElse(0);
        if (maximum == 0) {
            return Set.of();
        }
        Set<String> identifiers = new HashSet<>();
        int bulkSize = Math.max(maximum, this.bulkSize);
        try (PreparedStatement ps = conn.prepareStatement(
            "UPDATE" + (hint ? " /*+ INDEX (TASK TASK_BY_ID_IDX) */ " : " ") + "TASK "
                + "SET STATE = ? "
                + "WHERE TOPIC = ? "
                + "AND IDENTIFIER IN ("
                + "WITH JUNCTIONS (IDENTIFIER, MARKER, TOTAL) AS ("
                + String.join(" UNION ALL ", Collections.nCopies(bulkSize, "SELECT ?, ?, ? FROM DUAL"))
                + "), AGGREGATED (IDENTIFIER, ACTUAL, TOTAL) AS ("
                + "SELECT IDENTIFIER, COUNT(IDENTIFIER) OVER (PARTITION BY MARKER), TOTAL "
                + "FROM JUNCTIONS "
                + "WHERE IDENTIFIER IN ("
                + "SELECT IDENTIFIER "
                + "FROM TASK "
                + "WHERE TOPIC = ? "
                + "AND STATE BETWEEN ? AND ?"
                + ")"
                + ") "
                + "SELECT IDENTIFIER "
                + "FROM AGGREGATED "
                + "WHERE ACTUAL = TOTAL"
                + ") "
                + "AND STATE = ? "
                + "RETURNING IDENTIFIER INTO ?"
        )) {
            Iterator<Set<String>> outer = groups.iterator();
            Set<String> group = outer.next();
            Iterator<String> it = group.iterator();
            int marker = 0;
            do {
                ps.setInt(1, TaskState.READY.ordinal());
                ps.setString(2, topic);
                currentBulk:
                for (int index = 0; index < bulkSize; index++) {
                    while (!it.hasNext()) {
                        if (outer.hasNext()) {
                            group = outer.next();
                            it = group.iterator();
                            marker += 1;
                            if (group.size() > bulkSize - index) {
                                do {
                                    ps.setString(index * 3 + 3, null);
                                    ps.setInt(index * 3 + 4, -1);
                                    ps.setInt(index * 3 + 5, -1);
                                } while (index++ < bulkSize);
                                break currentBulk;
                            }
                        } else {
                            do {
                                ps.setString(index * 3 + 3, null);
                                ps.setInt(index * 3 + 4, -1);
                                ps.setInt(index * 3 + 5, -1);
                            } while (index++ < bulkSize);
                            break currentBulk;
                        }
                    }
                    ps.setString(index * 3 + 3, it.next());
                    ps.setInt(index * 3 + 4, marker);
                    ps.setInt(index * 3 + 5, group.size());
                }
                ps.setString(bulkSize * 3 + 3, topic);
                ps.setInt(bulkSize * 3 + 4, lower.ordinal());
                ps.setInt(bulkSize * 3 + 5, upper.ordinal());
                ps.setInt(bulkSize * 3 + 6, TaskState.SUSPENDED.ordinal());
                ps.unwrap(OraclePreparedStatement.class).registerReturnParameter(bulkSize * 3 + 7, Types.VARCHAR);
                if (ps.executeUpdate() > 0) {
                    try (ResultSet rs = ps.unwrap(OraclePreparedStatement.class).getReturnResultSet()) {
                        while (rs.next()) {
                            identifiers.add(rs.getString(1));
                        }
                    }
                }
            } while (outer.hasNext() || it.hasNext());
        }
        return identifiers;
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

    @Override
    String getResultsHint() {
        return hint ? " /*+ PARALLEL_INDEX(TASK "
            + "TASK_BY_COMPLETION_IDX"
            + " 35) PARALLEL(TASK 35) */ " : super.getResultsHint();
    }
}
