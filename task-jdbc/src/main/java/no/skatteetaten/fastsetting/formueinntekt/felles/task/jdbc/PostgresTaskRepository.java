package no.skatteetaten.fastsetting.formueinntekt.felles.task.jdbc;

import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.Task;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskCreation;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskState;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

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
    public boolean register(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO TASK_OWNER (OWNER) VALUES (?) ON CONFLICT DO NOTHING"
        )) {
            ps.setString(1, owner);
            return ps.executeUpdate() == 1;
        }
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
            return List.of();
        }
        Map<String, Long> duplicates;
        switch (insertion) {
            case APPEND:
                duplicates = null;
                break;
            case DELETE:
                Map<String, Long> counts = creations.stream().collect(Collectors.groupingBy(TaskCreation::getIdentifier, Collectors.counting()));
                creations = creations.stream().filter(creation -> counts.compute(
                    creation.getIdentifier(),
                    (ignored, value) -> --value
                ).intValue() == 0).collect(Collectors.toList());
                duplicates = null;
                break;
            default:
                duplicates = creations.stream().collect(Collectors.groupingBy(TaskCreation::getIdentifier, Collectors.counting()));
        }
        List<Task> tasks = new ArrayList<>(creations.size());
        List<TaskCreation> remaining = new ArrayList<>(creations);
        do {
            int bulkSize = Math.min((int) Math.pow(2, (int) (Math.log(remaining.size()) / Math.log(2))), 65_535 / 5);
            do {
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO TASK (TOPIC, IDENTIFIER, STATE, INPUT, REFERENCE) "
                                + "VALUES " + String.join(", ", Collections.nCopies(bulkSize, "(?, ?, ?, ?, ?)")) + " "
                                + "RETURNING SEQUENCE"
                )) {
                    for (int index = 0; index < bulkSize; index++) {
                        ps.setString(5 * index + 1, topic);
                        ps.setString(5 * index + 2, remaining.get(index).getIdentifier());
                        ps.setInt(5 * index + 3, (duplicates == null || duplicates.computeIfPresent(remaining.get(index).getIdentifier(), (ignored, count) -> --count) == 0
                                ? (remaining.get(index).isSuspended() ? TaskState.SUSPENDED : TaskState.READY)
                                : TaskState.REDUNDANT).ordinal());
                        ps.setString(5 * index + 4, remaining.get(index).getInput().orElse(null));
                        ps.setString(5 * index + 5, remaining.get(index).getReference().orElse(null));
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
        switch (insertion) {
            case REPLACE:
                doFilter(conn, topic, creations.stream().map(TaskCreation::getIdentifier).collect(Collectors.toSet()), tasks.get(0).getSequence(), TaskState.READY, TaskState.SUCCEEDED, false);
                break;
            case SUPERSEDE:
                doFilter(conn, topic, creations.stream().map(TaskCreation::getIdentifier).collect(Collectors.toSet()), tasks.get(0).getSequence(), TaskState.READY, TaskState.READY, false);
                break;
            case DELETE:
                doFilter(conn, topic, creations.stream().map(TaskCreation::getIdentifier).collect(Collectors.toSet()), tasks.get(0).getSequence(), TaskState.READY, TaskState.REDUNDANT, true);
                break;
        }
        return tasks;
    }

    @Override
    public Set<Task> poll(
        Connection conn, String topic, int size
    ) throws SQLException {
        if (size == 0) {
            return Set.of();
        }
        Set<Task> tasks = new LinkedHashSet<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "WITH POLLED (SEQUENCE) AS ("
                        + "SELECT SEQUENCE "
                        + "FROM TASK "
                        + "WHERE TOPIC = ? "
                        + "AND STATE = ? "
                        + "ORDER BY SEQUENCE "
                        + (concurrent ? "FOR UPDATE SKIP LOCKED " : "")
                        + "FETCH FIRST ? ROWS ONLY"
                        + ")"
                        + "UPDATE TASK "
                        + "SET STATE = ?, OWNER = ? "
                        + "WHERE TOPIC = ? "
                        + "AND SEQUENCE IN (SELECT SEQUENCE FROM POLLED) "
                        + "RETURNING SEQUENCE, IDENTIFIER, INPUT"
        )) {
            ps.setString(1, topic);
            ps.setInt(2, TaskState.READY.ordinal());
            ps.setInt(3, size);
            ps.setInt(4, TaskState.ACTIVE.ordinal());
            ps.setString(5, owner);
            ps.setString(6, topic);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
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
    void doFilter(Connection conn, String topic, Collection<String> identifiers, long sequence, TaskState from, TaskState to, boolean delete) throws SQLException {
        Set<String> checked = new HashSet<>();
        try (PreparedStatement ps = conn.prepareStatement(
            (delete ? "DELETE FROM TASK " : "UPDATE TASK SET STATE = ?, DESCENT = GREATEST(STATE, DESCENT) ")
                    + "WHERE TOPIC = ? "
                    + (concurrent ? "AND SEQUENCE IN (SELECT SEQUENCE FROM TASK WHERE TOPIC = ? " : "")
                    + "AND SEQUENCE < ? "
                    + "AND IDENTIFIER " + (identifiers.size() == 1 ? "= ?" : ("IN (" + String.join(", ", Collections.nCopies(bulkSize, "?")) + ")")) + " "
                    + "AND STATE BETWEEN ? AND ?"
                    + (concurrent ? " FOR UPDATE SKIP LOCKED)" : "")
        )) {
            if (!delete) {
                ps.setInt(1, TaskState.REDUNDANT.ordinal());
            }
            ps.setString(1 + (delete ? 0 : 1), topic);
            if (concurrent) {
                ps.setString(2 + (delete ? 0 : 1), topic);
            }
            ps.setLong(2 + (delete ? 0 : 1) + (concurrent ? 1 : 0), sequence);
            Iterator<String> it = identifiers.iterator();
            int index = 0;
            do {
                String identifier = it.next();
                if (checked.add(identifier)) {
                    ps.setString(++index + 2 + (delete ? 0 : 1) + (concurrent ? 1 : 0), identifier);
                }
                if (!it.hasNext()) {
                    while (index < bulkSize && identifiers.size() != 1) {
                        ps.setNull(++index + 2 + (delete ? 0 : 1) + (concurrent ? 1 : 0), Types.VARCHAR);
                    }
                }
                if (index == bulkSize || identifiers.size() == 1) {
                    ps.setInt(index + 3 + (delete ? 0 : 1) + (concurrent ? 1 : 0), from.ordinal());
                    ps.setInt(index + 4 + (delete ? 0 : 1) + (concurrent ? 1 : 0), to.ordinal());
                    ps.addBatch();
                    if (it.hasNext()) {
                        if (!delete) {
                            ps.setInt(1, TaskState.REDUNDANT.ordinal());
                        }
                        ps.setString(1 + (delete ? 0 : 1), topic);
                        if (concurrent) {
                            ps.setString(2 + (delete ? 0 : 1), topic);
                        }
                        ps.setLong(2 + (delete ? 0 : 1) + (concurrent ? 1 : 0), sequence);
                        index = 0;
                    }
                }
            } while (it.hasNext());
            ps.executeBatch();
        }
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
            "UPDATE TASK "
                    + "SET STATE = ? "
                    + "WHERE TOPIC = ? "
                    + "AND IDENTIFIER IN (SELECT IDENTIFIER FROM ("
                    + "SELECT IDENTIFIER, COUNT(IDENTIFIER) OVER (PARTITION BY MARKER) RESOLVED, TOTAL "
                    + "FROM (VALUES " + String.join(", ", Collections.nCopies(bulkSize, "(?, ?, ?)")) + ") "
                    + "AS JUNCTIONS (IDENTIFIER, MARKER, TOTAL) "
                    + "WHERE TOPIC = ? "
                    + "AND IDENTIFIER IN (SELECT IDENTIFIER FROM TASK) "
                    + "AND STATE BETWEEN ? AND ?"
                    + ") PRESELECT WHERE RESOLVED = TOTAL) "
                    + "AND STATE = ? "
                    + "RETURNING IDENTIFIER"
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
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        identifiers.add(rs.getString("IDENTIFIER"));
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
