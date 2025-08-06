package no.skatteetaten.fastsetting.formueinntekt.felles.task.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;
import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public abstract class AbstractTaskRepositoryTest<T extends DataSource> {

    private final long offset;

    private T dataSource;

    private TaskRepository<Connection, SQLException> repository;

    protected AbstractTaskRepositoryTest(long offset) {
        this.offset = offset;
    }

    @Before
    public void setUp() throws Exception {
        dataSource = dataSource();

        try (Connection conn = dataSource.getConnection()) {
            Liquibase liquibase = new Liquibase(JdbcTaskRepository.CHANGE_LOG,
                new ClassLoaderResourceAccessor(),
                DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(conn)));
            liquibase.update(new Contexts());
        }

        repository = getRepository("owner");
    }

    @After
    public void tearDown() throws Exception {
        shutdown(dataSource);
    }

    protected abstract T dataSource() throws SQLException;

    protected abstract void shutdown(T dataSource) throws SQLException;

    protected abstract TaskRepository<Connection, SQLException> getRepository(String owner);

    @Test
    public void can_prepare_database() throws Exception {
        dataSource.getConnection().close();
    }

    @Test
    public void can_initialize_twice() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            assertThat(repository.initialize(conn, "topic")).isTrue();
            assertThat(getRepository("other").initialize(conn, "topic")).isFalse();
        }
    }

    @Test
    public void can_discover_other_owners() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            assertThat(repository.register(conn)).isTrue();
            assertThat(getRepository("other").register(conn)).isTrue();
            assertThat(repository.owners(conn, 1, TimeUnit.SECONDS)).containsExactly("other");
            assertThat(repository.owners(conn, 0, TimeUnit.SECONDS)).isEmpty();
        }
    }

    @Test
    public void can_purge_expired_owners() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            assertThat(repository.register(conn)).isTrue();
            assertThat(getRepository("other").register(conn)).isTrue();
            assertThat(repository.purgeOwners(conn, 1, TimeUnit.SECONDS)).isEqualTo(0);
            assertThat(repository.purgeOwners(conn, 0, TimeUnit.SECONDS)).isEqualTo(1);
        }
    }

    @Test
    public void cannot_register_twice() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            assertThat(repository.register(conn)).isTrue();
            assertThat(repository.register(conn)).isFalse();
        }
    }

    @Test
    public void can_refresh() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            repository.refresh(conn);
        }
    }

    @Test
    public void can_push_append() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            assertThat(repository.initialize(conn, "topic")).isTrue();

            assertThat(repository.page(conn, "topic", TaskRepository.INCEPTION, Integer.MAX_VALUE)).isEmpty();

            Task task = repository.push(conn, "topic", TaskSink.Insertion.APPEND, "foo");
            assertThat(task.getIdentifier()).isEqualTo("foo");
            assertThat(task.getSequence()).isEqualTo(offset + 1);

            assertThat(repository.page(conn, "topic", new TaskRepository.Listing().withState(TaskState.READY), TaskRepository.INCEPTION, Integer.MAX_VALUE)).hasSize(1);
            assertThat(repository.page(conn, "topic", TaskRepository.INCEPTION, Integer.MAX_VALUE)).hasSize(1);

            assertThat(repository.task(conn, "topic", offset + 1)).hasValueSatisfying(info -> {
                assertThat(info.getState()).isEqualTo(TaskState.READY);
                assertThat(info.getDescent()).isEqualTo(TaskState.READY);
                assertThat(info.getSequence()).isEqualTo(offset + 1);
                assertThat(info.getIdentifier()).isEqualTo("foo");
                assertThat(info.getOwner()).isNotPresent();
                assertThat(info.getCreated()).isNotNull().isAfter(OffsetDateTime.now().minusMinutes(5));
                assertThat(info.getCompleted()).isNotPresent();
            });

            Task other = repository.push(conn, "topic", TaskSink.Insertion.APPEND, "foo");
            assertThat(other.getIdentifier()).isEqualTo("foo");
            assertThat(other.getSequence()).isEqualTo(offset + 2);

            assertThat(repository.page(conn, "topic", new TaskRepository.Listing().withState(TaskState.READY), TaskRepository.INCEPTION, Integer.MAX_VALUE)).hasSize(2);
            assertThat(repository.page(conn, "topic", TaskRepository.INCEPTION, Integer.MAX_VALUE)).hasSize(2);

            assertThat(repository.task(conn, "topic", offset + 1)).hasValueSatisfying(info -> {
                assertThat(info.getState()).isEqualTo(TaskState.READY);
                assertThat(info.getDescent()).isEqualTo(TaskState.READY);
                assertThat(info.getSequence()).isEqualTo(offset + 1);
                assertThat(info.getIdentifier()).isEqualTo("foo");
                assertThat(info.getOwner()).isNotPresent();
                assertThat(info.getCreated()).isNotNull().isAfter(OffsetDateTime.now().minusMinutes(5));
                assertThat(info.getCompleted()).isNotPresent();
            });

            assertThat(repository.task(conn, "topic", offset + 2)).hasValueSatisfying(info -> {
                assertThat(info.getState()).isEqualTo(TaskState.READY);
                assertThat(info.getDescent()).isEqualTo(TaskState.READY);
                assertThat(info.getSequence()).isEqualTo(offset + 2);
                assertThat(info.getIdentifier()).isEqualTo("foo");
                assertThat(info.getOwner()).isNotPresent();
                assertThat(info.getCreated()).isNotNull().isAfter(OffsetDateTime.now().minusMinutes(5));
                assertThat(info.getCompleted()).isNotPresent();
            });

            assertThat(repository.page(conn, "topic", new TaskRepository.Listing().withIdentifier("foo"), TaskRepository.INCEPTION, Integer.MAX_VALUE))
                .hasSize(2)
                .allSatisfy(historic -> assertThat(historic.getIdentifier()).isEqualTo("foo"))
                .extracting(TaskInfo::getSequence).contains(offset + 1, offset + 2);
        }
    }

    @Test
    public void can_push_replace() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            repository.register(conn);
            assertThat(repository.initialize(conn, "topic")).isTrue();

            assertThat(repository.page(conn, "topic", new TaskRepository.Listing().withState(TaskState.READY), TaskRepository.INCEPTION, Integer.MAX_VALUE)).isEmpty();
            assertThat(repository.page(conn, "topic", TaskRepository.INCEPTION, Integer.MAX_VALUE)).isEmpty();

            Task first = repository.push(conn, "topic", TaskSink.Insertion.REPLACE, "foo");
            assertThat(first.getIdentifier()).isEqualTo("foo");
            assertThat(first.getSequence()).isEqualTo(offset + 1);

            assertThat(repository.page(conn, "topic", new TaskRepository.Listing().withState(TaskState.READY), TaskRepository.INCEPTION, Integer.MAX_VALUE)).hasSize(1);
            assertThat(repository.page(conn, "topic", TaskRepository.INCEPTION, Integer.MAX_VALUE)).hasSize(1);

            assertThat(repository.task(conn, "topic", offset + 1)).hasValueSatisfying(info -> {
                assertThat(info.getState()).isEqualTo(TaskState.READY);
                assertThat(info.getDescent()).isEqualTo(TaskState.READY);
                assertThat(info.getSequence()).isEqualTo(offset + 1);
                assertThat(info.getIdentifier()).isEqualTo("foo");
                assertThat(info.getOwner()).isNotPresent();
                assertThat(info.getCreated()).isNotNull().isAfter(OffsetDateTime.now().minusMinutes(5));
                assertThat(info.getCompleted()).isNotPresent();
            });

            Task second = repository.push(conn, "topic", TaskSink.Insertion.REPLACE, "foo");
            assertThat(second.getIdentifier()).isEqualTo("foo");
            assertThat(second.getSequence()).isEqualTo(offset + 2);

            assertThat(repository.page(conn, "topic", new TaskRepository.Listing().withState(TaskState.READY), TaskRepository.INCEPTION, Integer.MAX_VALUE)).hasSize(1);
            assertThat(repository.page(conn, "topic", TaskRepository.INCEPTION, Integer.MAX_VALUE)).hasSize(2);

            assertThat(repository.task(conn, "topic", offset + 1)).hasValueSatisfying(info -> {
                assertThat(info.getState()).isEqualTo(TaskState.REDUNDANT);
                assertThat(info.getDescent()).isEqualTo(TaskState.READY);
                assertThat(info.getSequence()).isEqualTo(offset + 1);
                assertThat(info.getIdentifier()).isEqualTo("foo");
                assertThat(info.getOwner()).isNotPresent();
                assertThat(info.getCreated()).isNotNull().isAfter(OffsetDateTime.now().minusMinutes(5));
                assertThat(info.getCompleted()).isNotPresent();
            });

            assertThat(repository.task(conn, "topic", offset + 2)).hasValueSatisfying(info -> {
                assertThat(info.getState()).isEqualTo(TaskState.READY);
                assertThat(info.getDescent()).isEqualTo(TaskState.READY);
                assertThat(info.getSequence()).isEqualTo(offset + 2);
                assertThat(info.getIdentifier()).isEqualTo("foo");
                assertThat(info.getOwner()).isNotPresent();
                assertThat(info.getCreated()).isNotNull().isAfter(OffsetDateTime.now().minusMinutes(5));
                assertThat(info.getCompleted()).isNotPresent();
            });

            assertThat(repository.page(conn, "topic", new TaskRepository.Listing().withIdentifier("foo"), TaskRepository.INCEPTION, Integer.MAX_VALUE))
                .hasSize(2)
                .allSatisfy(historic -> assertThat(historic.getIdentifier()).isEqualTo("foo"))
                .extracting(TaskInfo::getSequence).contains(offset + 1, offset + 2);

            repository.complete(conn, "topic", repository.poll(conn, "topic").orElseThrow(), TaskDecision.SUCCESS);

            Task third = repository.push(conn, "topic", TaskSink.Insertion.REPLACE, "foo");
            assertThat(third.getIdentifier()).isEqualTo("foo");
            assertThat(third.getSequence()).isEqualTo(offset + 3);

            assertThat(repository.page(conn, "topic", new TaskRepository.Listing().withState(TaskState.READY), TaskRepository.INCEPTION, Integer.MAX_VALUE)).hasSize(1);
            assertThat(repository.page(conn, "topic", TaskRepository.INCEPTION, Integer.MAX_VALUE)).hasSize(3);

            assertThat(repository.task(conn, "topic", offset + 1)).hasValueSatisfying(info -> {
                assertThat(info.getState()).isEqualTo(TaskState.REDUNDANT);
                assertThat(info.getDescent()).isEqualTo(TaskState.READY);
                assertThat(info.getSequence()).isEqualTo(offset + 1);
                assertThat(info.getIdentifier()).isEqualTo("foo");
                assertThat(info.getOwner()).isNotPresent();
                assertThat(info.getCreated()).isNotNull().isAfter(OffsetDateTime.now().minusMinutes(5));
                assertThat(info.getCompleted()).isNotPresent();
            });

            assertThat(repository.task(conn, "topic", offset + 2)).hasValueSatisfying(info -> {
                assertThat(info.getState()).isEqualTo(TaskState.REDUNDANT);
                assertThat(info.getDescent()).isEqualTo(TaskState.SUCCEEDED);
                assertThat(info.getSequence()).isEqualTo(offset + 2);
                assertThat(info.getIdentifier()).isEqualTo("foo");
                assertThat(info.getOwner()).contains("owner");
                assertThat(info.getCreated()).isNotNull().isAfter(OffsetDateTime.now().minusMinutes(5));
                assertThat(info.getCompleted()).isPresent();
            });

            assertThat(repository.task(conn, "topic", offset + 3)).hasValueSatisfying(info -> {
                assertThat(info.getState()).isEqualTo(TaskState.READY);
                assertThat(info.getDescent()).isEqualTo(TaskState.READY);
                assertThat(info.getSequence()).isEqualTo(offset + 3);
                assertThat(info.getIdentifier()).isEqualTo("foo");
                assertThat(info.getOwner()).isNotPresent();
                assertThat(info.getCreated()).isNotNull().isAfter(OffsetDateTime.now().minusMinutes(5));
                assertThat(info.getCompleted()).isNotPresent();
            });

            assertThat(repository.page(conn, "topic", new TaskRepository.Listing().withIdentifier("foo"), TaskRepository.INCEPTION, Integer.MAX_VALUE))
                    .hasSize(3)
                    .allSatisfy(historic -> assertThat(historic.getIdentifier()).isEqualTo("foo"))
                    .extracting(TaskInfo::getSequence).contains(offset + 1, offset + 2, offset + 3);
        }
    }

    @Test
    public void can_push_supersede() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            repository.register(conn);
            assertThat(repository.initialize(conn, "topic")).isTrue();

            assertThat(repository.page(conn, "topic", new TaskRepository.Listing().withState(TaskState.READY), TaskRepository.INCEPTION, Integer.MAX_VALUE)).isEmpty();
            assertThat(repository.page(conn, "topic", TaskRepository.INCEPTION, Integer.MAX_VALUE)).isEmpty();

            Task first = repository.push(conn, "topic", TaskSink.Insertion.SUPERSEDE, "foo");
            assertThat(first.getIdentifier()).isEqualTo("foo");
            assertThat(first.getSequence()).isEqualTo(offset + 1);

            assertThat(repository.page(conn, "topic", new TaskRepository.Listing().withState(TaskState.READY), TaskRepository.INCEPTION, Integer.MAX_VALUE)).hasSize(1);
            assertThat(repository.page(conn, "topic", TaskRepository.INCEPTION, Integer.MAX_VALUE)).hasSize(1);

            assertThat(repository.task(conn, "topic", offset + 1)).hasValueSatisfying(info -> {
                assertThat(info.getState()).isEqualTo(TaskState.READY);
                assertThat(info.getDescent()).isEqualTo(TaskState.READY);
                assertThat(info.getSequence()).isEqualTo(offset + 1);
                assertThat(info.getIdentifier()).isEqualTo("foo");
                assertThat(info.getOwner()).isNotPresent();
                assertThat(info.getCreated()).isNotNull().isAfter(OffsetDateTime.now().minusMinutes(5));
                assertThat(info.getCompleted()).isNotPresent();
            });

            Task second = repository.push(conn, "topic", TaskSink.Insertion.SUPERSEDE, "foo");
            assertThat(second.getIdentifier()).isEqualTo("foo");
            assertThat(second.getSequence()).isEqualTo(offset + 2);

            assertThat(repository.page(conn, "topic", new TaskRepository.Listing().withState(TaskState.READY), TaskRepository.INCEPTION, Integer.MAX_VALUE)).hasSize(1);
            assertThat(repository.page(conn, "topic", TaskRepository.INCEPTION, Integer.MAX_VALUE)).hasSize(2);

            assertThat(repository.task(conn, "topic", offset + 1)).hasValueSatisfying(info -> {
                assertThat(info.getState()).isEqualTo(TaskState.REDUNDANT);
                assertThat(info.getDescent()).isEqualTo(TaskState.READY);
                assertThat(info.getSequence()).isEqualTo(offset + 1);
                assertThat(info.getIdentifier()).isEqualTo("foo");
                assertThat(info.getOwner()).isNotPresent();
                assertThat(info.getCreated()).isNotNull().isAfter(OffsetDateTime.now().minusMinutes(5));
                assertThat(info.getCompleted()).isNotPresent();
            });

            assertThat(repository.task(conn, "topic", offset + 2)).hasValueSatisfying(info -> {
                assertThat(info.getState()).isEqualTo(TaskState.READY);
                assertThat(info.getDescent()).isEqualTo(TaskState.READY);
                assertThat(info.getSequence()).isEqualTo(offset + 2);
                assertThat(info.getIdentifier()).isEqualTo("foo");
                assertThat(info.getOwner()).isNotPresent();
                assertThat(info.getCreated()).isNotNull().isAfter(OffsetDateTime.now().minusMinutes(5));
                assertThat(info.getCompleted()).isNotPresent();
            });

            assertThat(repository.page(conn, "topic", new TaskRepository.Listing().withIdentifier("foo"), TaskRepository.INCEPTION, Integer.MAX_VALUE))
                    .hasSize(2)
                    .allSatisfy(historic -> assertThat(historic.getIdentifier()).isEqualTo("foo"))
                    .extracting(TaskInfo::getSequence).contains(offset + 1, offset + 2);

            repository.complete(conn, "topic", repository.poll(conn, "topic").orElseThrow(), TaskDecision.SUCCESS);

            Task third = repository.push(conn, "topic", TaskSink.Insertion.SUPERSEDE, "foo");
            assertThat(third.getIdentifier()).isEqualTo("foo");
            assertThat(third.getSequence()).isEqualTo(offset + 3);

            assertThat(repository.page(conn, "topic", new TaskRepository.Listing().withState(TaskState.READY), TaskRepository.INCEPTION, Integer.MAX_VALUE)).hasSize(1);
            assertThat(repository.page(conn, "topic", TaskRepository.INCEPTION, Integer.MAX_VALUE)).hasSize(3);

            assertThat(repository.task(conn, "topic", offset + 1)).hasValueSatisfying(info -> {
                assertThat(info.getState()).isEqualTo(TaskState.REDUNDANT);
                assertThat(info.getDescent()).isEqualTo(TaskState.READY);
                assertThat(info.getSequence()).isEqualTo(offset + 1);
                assertThat(info.getIdentifier()).isEqualTo("foo");
                assertThat(info.getOwner()).isNotPresent();
                assertThat(info.getCreated()).isNotNull().isAfter(OffsetDateTime.now().minusMinutes(5));
                assertThat(info.getCompleted()).isNotPresent();
            });

            assertThat(repository.task(conn, "topic", offset + 2)).hasValueSatisfying(info -> {
                assertThat(info.getState()).isEqualTo(TaskState.SUCCEEDED);
                assertThat(info.getDescent()).isEqualTo(TaskState.READY);
                assertThat(info.getSequence()).isEqualTo(offset + 2);
                assertThat(info.getIdentifier()).isEqualTo("foo");
                assertThat(info.getOwner()).contains("owner");
                assertThat(info.getCreated()).isNotNull().isAfter(OffsetDateTime.now().minusMinutes(5));
                assertThat(info.getCompleted()).isPresent();
            });

            assertThat(repository.task(conn, "topic", offset + 3)).hasValueSatisfying(info -> {
                assertThat(info.getState()).isEqualTo(TaskState.READY);
                assertThat(info.getDescent()).isEqualTo(TaskState.READY);
                assertThat(info.getSequence()).isEqualTo(offset + 3);
                assertThat(info.getIdentifier()).isEqualTo("foo");
                assertThat(info.getOwner()).isNotPresent();
                assertThat(info.getCreated()).isNotNull().isAfter(OffsetDateTime.now().minusMinutes(5));
                assertThat(info.getCompleted()).isNotPresent();
            });

            assertThat(repository.page(conn, "topic", new TaskRepository.Listing().withIdentifier("foo"), TaskRepository.INCEPTION, Integer.MAX_VALUE))
                    .hasSize(3)
                    .allSatisfy(historic -> assertThat(historic.getIdentifier()).isEqualTo("foo"))
                    .extracting(TaskInfo::getSequence).contains(offset + 1, offset + 2, offset + 3);
        }
    }

    @Test
    public void can_push_delete() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            repository.register(conn);
            assertThat(repository.initialize(conn, "topic")).isTrue();

            assertThat(repository.page(conn, "topic", new TaskRepository.Listing().withState(TaskState.READY), TaskRepository.INCEPTION, Integer.MAX_VALUE)).isEmpty();
            assertThat(repository.page(conn, "topic", TaskRepository.INCEPTION, Integer.MAX_VALUE)).isEmpty();

            Task first = repository.push(conn, "topic", TaskSink.Insertion.DELETE, "foo");
            assertThat(first.getIdentifier()).isEqualTo("foo");
            assertThat(first.getSequence()).isEqualTo(offset + 1);

            assertThat(repository.page(conn, "topic", new TaskRepository.Listing().withState(TaskState.READY), TaskRepository.INCEPTION, Integer.MAX_VALUE)).hasSize(1);
            assertThat(repository.page(conn, "topic", TaskRepository.INCEPTION, Integer.MAX_VALUE)).hasSize(1);

            assertThat(repository.task(conn, "topic", offset + 1)).hasValueSatisfying(info -> {
                assertThat(info.getState()).isEqualTo(TaskState.READY);
                assertThat(info.getDescent()).isEqualTo(TaskState.READY);
                assertThat(info.getSequence()).isEqualTo(offset + 1);
                assertThat(info.getIdentifier()).isEqualTo("foo");
                assertThat(info.getOwner()).isNotPresent();
                assertThat(info.getCreated()).isNotNull().isAfter(OffsetDateTime.now().minusMinutes(5));
                assertThat(info.getCompleted()).isNotPresent();
            });

            Task second = repository.push(conn, "topic", TaskSink.Insertion.DELETE, "foo");
            assertThat(second.getIdentifier()).isEqualTo("foo");
            assertThat(second.getSequence()).isEqualTo(offset + 2);

            assertThat(repository.page(conn, "topic", new TaskRepository.Listing().withState(TaskState.READY), TaskRepository.INCEPTION, Integer.MAX_VALUE)).hasSize(1);
            assertThat(repository.page(conn, "topic", TaskRepository.INCEPTION, Integer.MAX_VALUE)).hasSize(1);

            assertThat(repository.task(conn, "topic", offset + 1)).isEmpty();

            assertThat(repository.task(conn, "topic", offset + 2)).hasValueSatisfying(info -> {
                assertThat(info.getState()).isEqualTo(TaskState.READY);
                assertThat(info.getDescent()).isEqualTo(TaskState.READY);
                assertThat(info.getSequence()).isEqualTo(offset + 2);
                assertThat(info.getIdentifier()).isEqualTo("foo");
                assertThat(info.getOwner()).isNotPresent();
                assertThat(info.getCreated()).isNotNull().isAfter(OffsetDateTime.now().minusMinutes(5));
                assertThat(info.getCompleted()).isNotPresent();
            });

            assertThat(repository.page(conn, "topic", new TaskRepository.Listing().withIdentifier("foo"), TaskRepository.INCEPTION, Integer.MAX_VALUE))
                    .hasSize(1)
                    .allSatisfy(historic -> assertThat(historic.getIdentifier()).isEqualTo("foo"))
                    .extracting(TaskInfo::getSequence).contains(offset + 2);

            repository.complete(conn, "topic", repository.poll(conn, "topic").orElseThrow(), TaskDecision.SUCCESS);

            Task third = repository.push(conn, "topic", TaskSink.Insertion.DELETE, "foo");
            assertThat(third.getIdentifier()).isEqualTo("foo");
            assertThat(third.getSequence()).isEqualTo(offset + 3);

            assertThat(repository.page(conn, "topic", new TaskRepository.Listing().withState(TaskState.READY), TaskRepository.INCEPTION, Integer.MAX_VALUE)).hasSize(1);
            assertThat(repository.page(conn, "topic", TaskRepository.INCEPTION, Integer.MAX_VALUE)).hasSize(1);

            assertThat(repository.task(conn, "topic", offset + 1)).isEmpty();

            assertThat(repository.task(conn, "topic", offset + 2)).isEmpty();

            assertThat(repository.task(conn, "topic", offset + 3)).hasValueSatisfying(info -> {
                assertThat(info.getState()).isEqualTo(TaskState.READY);
                assertThat(info.getDescent()).isEqualTo(TaskState.READY);
                assertThat(info.getSequence()).isEqualTo(offset + 3);
                assertThat(info.getIdentifier()).isEqualTo("foo");
                assertThat(info.getOwner()).isNotPresent();
                assertThat(info.getCreated()).isNotNull().isAfter(OffsetDateTime.now().minusMinutes(5));
                assertThat(info.getCompleted()).isNotPresent();
            });

            assertThat(repository.page(conn, "topic", new TaskRepository.Listing().withIdentifier("foo"), TaskRepository.INCEPTION, Integer.MAX_VALUE))
                    .hasSize(1)
                    .allSatisfy(historic -> assertThat(historic.getIdentifier()).isEqualTo("foo"))
                    .extracting(TaskInfo::getSequence).contains(offset + 3);
        }
    }

    @Test
    public void can_push_suspended() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            assertThat(repository.initialize(conn, "topic")).isTrue();

            assertThat(repository.page(conn, "topic", new TaskRepository.Listing().withState(TaskState.READY), TaskRepository.INCEPTION, Integer.MAX_VALUE)).isEmpty();
            assertThat(repository.page(conn, "topic", TaskRepository.INCEPTION, Integer.MAX_VALUE)).isEmpty();

            Task task = repository.push(conn, "topic", TaskSink.Insertion.REPLACE, new TaskCreation("foo").withSuspension(true));
            assertThat(task.getIdentifier()).isEqualTo("foo");
            assertThat(task.getSequence()).isEqualTo(offset + 1);

            assertThat(repository.page(conn, "topic", new TaskRepository.Listing().withState(TaskState.SUSPENDED), TaskRepository.INCEPTION, Integer.MAX_VALUE)).hasSize(1);
            assertThat(repository.page(conn, "topic", TaskRepository.INCEPTION, Integer.MAX_VALUE)).hasSize(1);

            assertThat(repository.task(conn, "topic", offset + 1)).hasValueSatisfying(info -> {
                assertThat(info.getState()).isEqualTo(TaskState.SUSPENDED);
                assertThat(info.getDescent()).isEqualTo(TaskState.READY);
                assertThat(info.getSequence()).isEqualTo(offset + 1);
                assertThat(info.getIdentifier()).isEqualTo("foo");
                assertThat(info.getOwner()).isNotPresent();
                assertThat(info.getCreated()).isNotNull().isAfter(OffsetDateTime.now().minusMinutes(5));
                assertThat(info.getCompleted()).isNotPresent();
            });

            Task other = repository.push(conn, "topic", TaskSink.Insertion.REPLACE, new TaskCreation("foo").withSuspension(true));
            assertThat(other.getIdentifier()).isEqualTo("foo");
            assertThat(other.getSequence()).isEqualTo(offset + 2);

            assertThat(repository.page(conn, "topic", new TaskRepository.Listing().withState(TaskState.SUSPENDED), TaskRepository.INCEPTION, Integer.MAX_VALUE)).hasSize(1);
            assertThat(repository.page(conn, "topic", TaskRepository.INCEPTION, Integer.MAX_VALUE)).hasSize(2);

            assertThat(repository.task(conn, "topic", offset + 1)).hasValueSatisfying(info -> {
                assertThat(info.getState()).isEqualTo(TaskState.REDUNDANT);
                assertThat(info.getDescent()).isEqualTo(TaskState.SUSPENDED);
                assertThat(info.getSequence()).isEqualTo(offset + 1);
                assertThat(info.getIdentifier()).isEqualTo("foo");
                assertThat(info.getOwner()).isNotPresent();
                assertThat(info.getCreated()).isNotNull().isAfter(OffsetDateTime.now().minusMinutes(5));
                assertThat(info.getCompleted()).isNotPresent();
            });

            assertThat(repository.task(conn, "topic", offset + 2)).hasValueSatisfying(info -> {
                assertThat(info.getState()).isEqualTo(TaskState.SUSPENDED);
                assertThat(info.getDescent()).isEqualTo(TaskState.READY);
                assertThat(info.getSequence()).isEqualTo(offset + 2);
                assertThat(info.getIdentifier()).isEqualTo("foo");
                assertThat(info.getOwner()).isNotPresent();
                assertThat(info.getCreated()).isNotNull().isAfter(OffsetDateTime.now().minusMinutes(5));
                assertThat(info.getCompleted()).isNotPresent();
            });

            assertThat(repository.page(conn, "topic", new TaskRepository.Listing().withIdentifier("foo"), TaskRepository.INCEPTION, Integer.MAX_VALUE))
                    .hasSize(2)
                    .allSatisfy(historic -> assertThat(historic.getIdentifier()).isEqualTo("foo"))
                    .extracting(TaskInfo::getSequence).contains(offset + 1, offset + 2);
        }
    }

    @Test
    public void cannot_push_unknown_topic() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            assertThat(repository.initialize(conn, "topic")).isTrue();

            assertThatThrownBy(
                () -> repository.push(conn, "other", TaskSink.Insertion.REPLACE, "foo")
            ).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    public void can_poll_first_in_first_out() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            assertThat(repository.register(conn)).isTrue();
            assertThat(repository.initialize(conn, "topic")).isTrue();

            assertThat(repository.poll(conn, "topic", TaskSource.Order.FIRST_IN_FIRST_OUT)).isNotPresent();

            List<Task> tasks = repository.push(conn, "topic", TaskSink.Insertion.APPEND, "foo", "bar", "qux");
            assertThat(tasks).hasSize(3);
            assertThat(tasks.get(0).getIdentifier()).isEqualTo("foo");
            assertThat(tasks.get(0).getSequence()).isEqualTo(offset + 1);
            assertThat(tasks.get(1).getIdentifier()).isEqualTo("bar");
            assertThat(tasks.get(1).getSequence()).isEqualTo(offset + 2);
            assertThat(tasks.get(2).getIdentifier()).isEqualTo("qux");
            assertThat(tasks.get(2).getSequence()).isEqualTo(offset + 3);

            assertThat(repository.poll(conn, "topic", TaskSource.Order.FIRST_IN_FIRST_OUT, Integer.MAX_VALUE)).containsExactly(
                new Task(offset + 1, "foo"),
                new Task(offset + 2, "bar"),
                new Task(offset + 3, "qux")
            );
        }
    }

    @Test
    public void can_poll_last_in_first_out() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            assertThat(repository.register(conn)).isTrue();
            assertThat(repository.initialize(conn, "topic")).isTrue();

            assertThat(repository.poll(conn, "topic", TaskSource.Order.LAST_IN_FIRST_OUT)).isNotPresent();

            List<Task> tasks = repository.push(conn, "topic", TaskSink.Insertion.APPEND, "foo", "bar", "qux");
            assertThat(tasks).hasSize(3);
            assertThat(tasks.get(0).getIdentifier()).isEqualTo("foo");
            assertThat(tasks.get(0).getSequence()).isEqualTo(offset + 1);
            assertThat(tasks.get(1).getIdentifier()).isEqualTo("bar");
            assertThat(tasks.get(1).getSequence()).isEqualTo(offset + 2);
            assertThat(tasks.get(2).getIdentifier()).isEqualTo("qux");
            assertThat(tasks.get(2).getSequence()).isEqualTo(offset + 3);

            assertThat(repository.poll(conn, "topic", TaskSource.Order.LAST_IN_FIRST_OUT, Integer.MAX_VALUE)).containsExactly(
                new Task(offset + 3, "qux"),
                new Task(offset + 2, "bar"),
                new Task(offset + 1, "foo")
            );
        }
    }

    @Test
    public void can_poll_first_in_first_out_singular_by_identifier() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            assertThat(repository.register(conn)).isTrue();
            assertThat(repository.initialize(conn, "topic")).isTrue();

            assertThat(repository.poll(conn, "topic", TaskSource.Order.FIRST_IN_FIRST_OUT, TaskSource.Condition.SINGULAR_BY_IDENTIFIER)).isNotPresent();

            List<Task> tasks = repository.push(conn, "topic", TaskSink.Insertion.APPEND, "foo", "bar", "foo");
            assertThat(tasks).hasSize(3);
            assertThat(tasks.get(0).getIdentifier()).isEqualTo("foo");
            assertThat(tasks.get(0).getSequence()).isEqualTo(offset + 1);
            assertThat(tasks.get(1).getIdentifier()).isEqualTo("bar");
            assertThat(tasks.get(1).getSequence()).isEqualTo(offset + 2);
            assertThat(tasks.get(2).getIdentifier()).isEqualTo("foo");
            assertThat(tasks.get(2).getSequence()).isEqualTo(offset + 3);

            assertThat(repository.poll(conn, "topic", TaskSource.Order.FIRST_IN_FIRST_OUT, TaskSource.Condition.SINGULAR_BY_IDENTIFIER, Integer.MAX_VALUE)).containsExactly(
                new Task(offset + 1, "foo"),
                new Task(offset + 2, "bar")
            );

            assertThat(repository.poll(conn, "topic", TaskSource.Order.FIRST_IN_FIRST_OUT, TaskSource.Condition.SINGULAR_BY_IDENTIFIER, Integer.MAX_VALUE)).isEmpty();
        }
    }

    @Test
    public void can_poll_last_in_first_out_singular_by_identifier() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            assertThat(repository.register(conn)).isTrue();
            assertThat(repository.initialize(conn, "topic")).isTrue();

            assertThat(repository.poll(conn, "topic", TaskSource.Order.LAST_IN_FIRST_OUT, TaskSource.Condition.SINGULAR_BY_IDENTIFIER)).isNotPresent();

            List<Task> tasks = repository.push(conn, "topic", TaskSink.Insertion.APPEND, "foo", "bar", "foo");
            assertThat(tasks).hasSize(3);
            assertThat(tasks.get(0).getIdentifier()).isEqualTo("foo");
            assertThat(tasks.get(0).getSequence()).isEqualTo(offset + 1);
            assertThat(tasks.get(1).getIdentifier()).isEqualTo("bar");
            assertThat(tasks.get(1).getSequence()).isEqualTo(offset + 2);
            assertThat(tasks.get(2).getIdentifier()).isEqualTo("foo");
            assertThat(tasks.get(2).getSequence()).isEqualTo(offset + 3);

            assertThat(repository.poll(conn, "topic", TaskSource.Order.LAST_IN_FIRST_OUT, TaskSource.Condition.SINGULAR_BY_IDENTIFIER, Integer.MAX_VALUE)).containsExactly(
                new Task(offset + 3, "foo"),
                new Task(offset + 2, "bar")
            );

            assertThat(repository.poll(conn, "topic", TaskSource.Order.LAST_IN_FIRST_OUT, TaskSource.Condition.SINGULAR_BY_IDENTIFIER, Integer.MAX_VALUE)).isEmpty();
        }
    }

    @Test
    public void can_poll_singular_by_identifier_suspend_on_failure() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            assertThat(repository.register(conn)).isTrue();
            assertThat(repository.initialize(conn, "topic")).isTrue();

            assertThat(repository.poll(conn, "topic", TaskSource.Condition.SINGULAR_BY_IDENTIFIER_SUSPEND_ON_FAILURE)).isNotPresent();

            List<Task> tasks = repository.push(conn, "topic", TaskSink.Insertion.APPEND, "foo", "bar", "foo");
            assertThat(tasks).hasSize(3);
            assertThat(tasks.get(0).getIdentifier()).isEqualTo("foo");
            assertThat(tasks.get(0).getSequence()).isEqualTo(offset + 1);
            assertThat(tasks.get(1).getIdentifier()).isEqualTo("bar");
            assertThat(tasks.get(1).getSequence()).isEqualTo(offset + 2);
            assertThat(tasks.get(2).getIdentifier()).isEqualTo("foo");
            assertThat(tasks.get(2).getSequence()).isEqualTo(offset + 3);

            assertThat(repository.poll(conn, "topic")).contains(
                new Task(offset + 1, "foo")
            );

            repository.complete(conn, "topic", new Task(offset + 1, "foo"), TaskDecision.FAILURE);

            assertThat(repository.poll(conn, "topic", TaskSource.Condition.SINGULAR_BY_IDENTIFIER_SUSPEND_ON_FAILURE, Integer.MAX_VALUE)).containsExactly(
                new Task(offset + 2, "bar")
            );

            assertThat(repository.poll(conn, "topic", TaskSource.Condition.SINGULAR_BY_IDENTIFIER_SUSPEND_ON_FAILURE, Integer.MAX_VALUE)).isEmpty();
        }
    }

    @Test
    public void can_poll_singular_by_identifier_suspend_until_succes() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            assertThat(repository.register(conn)).isTrue();
            assertThat(repository.initialize(conn, "topic")).isTrue();

            assertThat(repository.poll(conn, "topic", TaskSource.Condition.SINGULAR_BY_IDENTIFIER_SUSPEND_UNTIL_SUCCESS)).isNotPresent();

            List<Task> tasks = repository.push(conn, "topic", TaskSink.Insertion.APPEND, "foo", "bar", "foo");
            assertThat(tasks).hasSize(3);
            assertThat(tasks.get(0).getIdentifier()).isEqualTo("foo");
            assertThat(tasks.get(0).getSequence()).isEqualTo(offset + 1);
            assertThat(tasks.get(1).getIdentifier()).isEqualTo("bar");
            assertThat(tasks.get(1).getSequence()).isEqualTo(offset + 2);
            assertThat(tasks.get(2).getIdentifier()).isEqualTo("foo");
            assertThat(tasks.get(2).getSequence()).isEqualTo(offset + 3);

            assertThat(repository.poll(conn, "topic")).contains(
                new Task(offset + 1, "foo")
            );

            repository.complete(conn, "topic", new Task(offset + 1, "foo"), TaskDecision.SUSPENSION);

            assertThat(repository.poll(conn, "topic", TaskSource.Condition.SINGULAR_BY_IDENTIFIER_SUSPEND_UNTIL_SUCCESS, Integer.MAX_VALUE)).containsExactly(
                new Task(offset + 2, "bar")
            );

            assertThat(repository.poll(conn, "topic", TaskSource.Condition.SINGULAR_BY_IDENTIFIER_SUSPEND_UNTIL_SUCCESS, Integer.MAX_VALUE)).isEmpty();
        }
    }

    @Test
    public void can_poll_and_complete() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            assertThat(repository.register(conn)).isTrue();
            assertThat(repository.initialize(conn, "topic")).isTrue();

            assertThat(repository.poll(conn, "topic")).isNotPresent();

            Task task = repository.push(conn, "topic", TaskSink.Insertion.APPEND, "foo");
            assertThat(task.getIdentifier()).isEqualTo("foo");
            assertThat(task.getSequence()).isEqualTo(offset + 1);

            assertThat(repository.poll(conn, "topic")).contains(task);

            assertThat(repository.task(conn, "topic", offset + 1)).hasValueSatisfying(info -> {
                assertThat(info.getState()).isEqualTo(TaskState.ACTIVE);
                assertThat(info.getDescent()).isEqualTo(TaskState.READY);
                assertThat(info.getSequence()).isEqualTo(offset + 1);
                assertThat(info.getIdentifier()).isEqualTo("foo");
                assertThat(info.getOwner()).contains("owner");
                assertThat(info.getCreated()).isNotNull().isAfter(OffsetDateTime.now().minusMinutes(5));
                assertThat(info.getCompleted()).isNotPresent();
            });

            repository.complete(conn, "topic", task, TaskDecision.SUCCESS);

            assertThat(repository.task(conn, "topic", offset + 1)).hasValueSatisfying(info -> {
                assertThat(info.getState()).isEqualTo(TaskState.SUCCEEDED);
                assertThat(info.getDescent()).isEqualTo(TaskState.READY);
                assertThat(info.getSequence()).isEqualTo(offset + 1);
                assertThat(info.getIdentifier()).isEqualTo("foo");
                assertThat(info.getOwner()).contains("owner");
                assertThat(info.getCreated()).isNotNull().isAfter(OffsetDateTime.now().minusMinutes(5));
                assertThat(info.getCompleted()).isPresent().hasValueSatisfying(dateTime -> assertThat(dateTime).isAfter(info.getCreated()));
            });
        }
    }

    @Test
    public void can_poll_and_complete_with_payload() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            assertThat(repository.register(conn)).isTrue();
            assertThat(repository.initialize(conn, "topic")).isTrue();

            assertThat(repository.poll(conn, "topic")).isNotPresent();

            Task task = repository.push(conn, "topic", TaskSink.Insertion.APPEND, new TaskCreation("foo", "input"));
            assertThat(task.getIdentifier()).isEqualTo("foo");
            assertThat(task.getSequence()).isEqualTo(offset + 1);
            assertThat(task.getInput()).contains("input");

            assertThat(repository.poll(conn, "topic")).contains(task);

            assertThat(repository.task(conn, "topic", offset + 1)).hasValueSatisfying(info -> {
                assertThat(info.getState()).isEqualTo(TaskState.ACTIVE);
                assertThat(info.getDescent()).isEqualTo(TaskState.READY);
                assertThat(info.getSequence()).isEqualTo(offset + 1);
                assertThat(info.getIdentifier()).isEqualTo("foo");
                assertThat(info.getOwner()).contains("owner");
                assertThat(info.getCreated()).isNotNull().isAfter(OffsetDateTime.now().minusMinutes(5));
                assertThat(info.getCompleted()).isNotPresent();
                assertThat(info.getInput()).contains("input");
                assertThat(info.getOutput()).isEmpty();
            });

            repository.complete(conn, "topic", task, new TaskDecision(TaskResult.SUCCESS, "output"));

            assertThat(repository.task(conn, "topic", offset + 1)).hasValueSatisfying(info -> {
                assertThat(info.getState()).isEqualTo(TaskState.SUCCEEDED);
                assertThat(info.getDescent()).isEqualTo(TaskState.READY);
                assertThat(info.getSequence()).isEqualTo(offset + 1);
                assertThat(info.getIdentifier()).isEqualTo("foo");
                assertThat(info.getOwner()).contains("owner");
                assertThat(info.getCreated()).isNotNull().isAfter(OffsetDateTime.now().minusMinutes(5));
                assertThat(info.getCompleted()).isPresent().hasValueSatisfying(dateTime -> assertThat(dateTime).isAfter(info.getCreated()));
                assertThat(info.getInput()).contains("input");
                assertThat(info.getOutput()).contains("output");
            });
        }
    }

    @Test
    public void cannot_complete_without_poll() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            assertThat(repository.register(conn)).isTrue();
            assertThat(repository.initialize(conn, "topic")).isTrue();

            Task task = repository.push(conn, "topic", TaskSink.Insertion.APPEND, "foo");
            assertThat(task.getIdentifier()).isEqualTo("foo");
            assertThat(task.getSequence()).isEqualTo(offset + 1);

            assertThatThrownBy(
                () -> repository.complete(conn, "topic", task, TaskDecision.SUCCESS)
            ).isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    public void cannot_complete_polled_different_owner() throws Exception {
        TaskRepository<Connection, SQLException> other = getRepository("other");
        try (Connection conn = dataSource.getConnection()) {
            assertThat(repository.register(conn)).isTrue();
            assertThat(other.register(conn)).isTrue();
            assertThat(repository.initialize(conn, "topic")).isTrue();
            assertThat(other.initialize(conn, "topic")).isFalse();

            Task task = repository.push(conn, "topic", TaskSink.Insertion.APPEND, "foo");
            assertThat(task.getIdentifier()).isEqualTo("foo");
            assertThat(task.getSequence()).isEqualTo(offset + 1);

            assertThat(other.poll(conn, "topic")).contains(task);

            assertThatThrownBy(
                () -> repository.complete(conn, "topic", task, TaskDecision.SUCCESS)
            ).isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    public void can_recreate_append() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            assertThat(repository.register(conn)).isTrue();
            assertThat(repository.initialize(conn, "topic")).isTrue();

            Task task = repository.push(conn, "topic", TaskSink.Insertion.APPEND, "foo");
            assertThat(task.getIdentifier()).isEqualTo("foo");
            assertThat(task.getSequence()).isEqualTo(offset + 1);

            Task other = repository.push(conn, "topic", TaskSink.Insertion.APPEND, "foo");
            assertThat(other.getIdentifier()).isEqualTo("foo");
            assertThat(other.getSequence()).isEqualTo(offset + 2);

            repository.complete(
                conn,
                "topic",
                repository.poll(conn, "topic").orElseThrow(AssertionError::new),
                TaskDecision.FAILURE
            );

            Task recreation = repository.recreate(
                conn,
                "topic",
                TaskReviver.Revivification.APPEND,
                TaskReviver.Revived.FAILED
            ).orElseThrow(AssertionError::new);

            assertThat(recreation.getIdentifier()).isEqualTo("foo");
            assertThat(recreation.getSequence()).isEqualTo(offset + 3);

            assertThat(repository.task(conn, "topic", offset + 2).map(TaskInfo::getState)).contains(TaskState.READY);
        }
    }

    @Test
    public void can_recreate_replace() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            assertThat(repository.register(conn)).isTrue();
            assertThat(repository.initialize(conn, "topic")).isTrue();

            Task task = repository.push(conn, "topic", TaskSink.Insertion.APPEND, "foo");
            assertThat(task.getIdentifier()).isEqualTo("foo");
            assertThat(task.getSequence()).isEqualTo(offset + 1);

            Task other = repository.push(conn, "topic", TaskSink.Insertion.APPEND, "foo");
            assertThat(other.getIdentifier()).isEqualTo("foo");
            assertThat(other.getSequence()).isEqualTo(offset + 2);

            repository.complete(
                conn,
                "topic",
                repository.poll(conn, "topic").orElseThrow(AssertionError::new),
                TaskDecision.FAILURE
            );

            Task recreation = repository.recreate(
                conn,
                "topic",
                TaskReviver.Revivification.REPLACE,
                TaskReviver.Revived.FAILED
            ).orElseThrow(AssertionError::new);

            assertThat(recreation.getIdentifier()).isEqualTo("foo");
            assertThat(recreation.getSequence()).isEqualTo(offset + 3);

            assertThat(repository.task(conn, "topic", offset + 2).map(TaskInfo::getState)).contains(TaskState.REDUNDANT);
        }
    }

    @Test
    public void can_recreate_superseed() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            assertThat(repository.register(conn)).isTrue();
            assertThat(repository.initialize(conn, "topic")).isTrue();

            Task task = repository.push(conn, "topic", TaskSink.Insertion.APPEND, "foo");
            assertThat(task.getIdentifier()).isEqualTo("foo");
            assertThat(task.getSequence()).isEqualTo(offset + 1);

            Task other = repository.push(conn, "topic", TaskSink.Insertion.APPEND, "foo");
            assertThat(other.getIdentifier()).isEqualTo("foo");
            assertThat(other.getSequence()).isEqualTo(offset + 2);

            repository.complete(
                    conn,
                    "topic",
                    repository.poll(conn, "topic").orElseThrow(AssertionError::new),
                    TaskDecision.FAILURE
            );

            Task recreation = repository.recreate(
                    conn,
                    "topic",
                    TaskReviver.Revivification.SUPERSEDE,
                    TaskReviver.Revived.FAILED
            ).orElseThrow(AssertionError::new);

            assertThat(recreation.getIdentifier()).isEqualTo("foo");
            assertThat(recreation.getSequence()).isEqualTo(offset + 3);

            assertThat(repository.task(conn, "topic", offset + 2).map(TaskInfo::getState)).contains(TaskState.REDUNDANT);
        }
    }

    @Test
    public void can_recreate_reset() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            assertThat(repository.register(conn)).isTrue();
            assertThat(repository.initialize(conn, "topic")).isTrue();

            Task task = repository.push(conn, "topic", TaskSink.Insertion.APPEND, "foo");
            assertThat(task.getIdentifier()).isEqualTo("foo");
            assertThat(task.getSequence()).isEqualTo(offset + 1);

            Task other = repository.push(conn, "topic", TaskSink.Insertion.APPEND, "foo");
            assertThat(other.getIdentifier()).isEqualTo("foo");
            assertThat(other.getSequence()).isEqualTo(offset + 2);

            repository.complete(
                conn,
                "topic",
                repository.poll(conn, "topic").orElseThrow(AssertionError::new),
                TaskDecision.FAILURE
            );

            Task recreation = repository.recreate(
                conn,
                "topic",
                TaskReviver.Revivification.RESET,
                TaskReviver.Revived.FAILED
            ).orElseThrow(AssertionError::new);

            assertThat(recreation.getIdentifier()).isEqualTo("foo");
            assertThat(recreation.getSequence()).isEqualTo(offset + 1);

            assertThat(repository.task(conn, "topic", offset + 2).map(TaskInfo::getState)).contains(TaskState.READY);
        }
    }

    @Test
    public void can_reassign() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            assertThat(repository.register(conn)).isTrue();
            assertThat(repository.initialize(conn, "topic")).isTrue();

            Task task = repository.push(conn, "topic", TaskSink.Insertion.APPEND, "foo");
            assertThat(task.getIdentifier()).isEqualTo("foo");
            assertThat(task.getSequence()).isEqualTo(offset + 1);

            assertThatThrownBy(() -> repository.reassign(conn, "topic", task, TaskResult.FAILURE)).isInstanceOf(IllegalStateException.class);

            repository.complete(conn, "topic", repository.poll(conn, "topic").orElseThrow(), TaskDecision.SUCCESS);
            assertThat(repository.task(conn, "topic", task.getSequence()).map(TaskInfo::getState)).contains(TaskState.SUCCEEDED);

            repository.reassign(conn, "topic", task, TaskResult.FAILURE);
            assertThat(repository.task(conn, "topic", task.getSequence()).map(TaskInfo::getState)).contains(TaskState.FAILED);
        }
    }

    @Test
    public void can_reassign_all() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            assertThat(repository.register(conn)).isTrue();
            assertThat(repository.initialize(conn, "topic")).isTrue();

            Task task = repository.push(conn, "topic", TaskSink.Insertion.APPEND, "foo");
            assertThat(task.getIdentifier()).isEqualTo("foo");
            assertThat(task.getSequence()).isEqualTo(offset + 1);

            repository.complete(conn, "topic", repository.poll(conn, "topic").orElseThrow(), TaskDecision.SUCCESS);
            assertThat(repository.task(conn, "topic", task.getSequence()).map(TaskInfo::getState)).contains(TaskState.SUCCEEDED);

            assertThat(repository.reassignAll(conn, "topic", TaskReviver.Revived.SUCCEEDED, TaskResult.FAILURE)).isEqualTo(1);
            assertThat(repository.reassignAll(conn, "topic", TaskReviver.Revived.SUCCEEDED, TaskResult.FAILURE)).isEqualTo(0);
            assertThat(repository.task(conn, "topic", task.getSequence()).map(TaskInfo::getState)).contains(TaskState.FAILED);
        }
    }

    @Test
    public void can_resolve_junction_singular() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            assertThat(repository.register(conn)).isTrue();
            assertThat(repository.initialize(conn, "topic")).isTrue();

            Task first = repository.push(conn, "topic", TaskSink.Insertion.APPEND, new TaskCreation("foo").withSuspension(true));
            assertThat(first.getIdentifier()).isEqualTo("foo");
            assertThat(first.getSequence()).isEqualTo(offset + 1);

            Task second = repository.push(conn, "topic", TaskSink.Insertion.APPEND, new TaskCreation("bar").withSuspension(true));
            assertThat(second.getIdentifier()).isEqualTo("bar");
            assertThat(second.getSequence()).isEqualTo(offset + 2);

            assertThat(repository.junction(conn, "topic", TaskJuncture.Junction.SINGULAR, "foo", "bar")).isTrue();

            assertThat(repository.task(conn, "topic", offset + 1).orElseThrow().getState()).isEqualTo(TaskState.READY);
            assertThat(repository.task(conn, "topic", offset + 2).orElseThrow().getState()).isEqualTo(TaskState.READY);
        }
    }

    @Test
    public void can_resolve_junction_eternal() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            assertThat(repository.register(conn)).isTrue();
            assertThat(repository.initialize(conn, "topic")).isTrue();

            Task first = repository.push(conn, "topic", TaskSink.Insertion.APPEND, new TaskCreation("foo"));
            assertThat(first.getIdentifier()).isEqualTo("foo");
            assertThat(first.getSequence()).isEqualTo(offset + 1);

            Task second = repository.push(conn, "topic", TaskSink.Insertion.APPEND, new TaskCreation("bar").withSuspension(true));
            assertThat(second.getIdentifier()).isEqualTo("bar");
            assertThat(second.getSequence()).isEqualTo(offset + 2);

            Task task = repository.poll(conn, "topic").orElseThrow();
            repository.complete(conn, "topic", task, TaskDecision.SUCCESS);
            assertThat(repository.junction(conn, "topic", TaskJuncture.Junction.ETERNAL, "foo", "bar")).isTrue();

            assertThat(repository.task(conn, "topic", offset + 1).orElseThrow().getState()).isEqualTo(TaskState.SUCCEEDED);
            assertThat(repository.task(conn, "topic", offset + 2).orElseThrow().getState()).isEqualTo(TaskState.READY);
        }
    }

    @Test
    public void can_resolve_partial_junction() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            assertThat(repository.register(conn)).isTrue();
            assertThat(repository.initialize(conn, "topic")).isTrue();

            Task first = repository.push(conn, "topic", TaskSink.Insertion.APPEND, new TaskCreation("foo").withSuspension(true));
            assertThat(first.getIdentifier()).isEqualTo("foo");
            assertThat(first.getSequence()).isEqualTo(offset + 1);

            Task second = repository.push(conn, "topic", TaskSink.Insertion.APPEND, new TaskCreation("bar").withSuspension(true));
            assertThat(second.getIdentifier()).isEqualTo("bar");
            assertThat(second.getSequence()).isEqualTo(offset + 2);

            Task third = repository.push(conn, "topic", TaskSink.Insertion.APPEND, new TaskCreation("qux").withSuspension(true));
            assertThat(third.getIdentifier()).isEqualTo("qux");
            assertThat(third.getSequence()).isEqualTo(offset + 3);

            assertThat(repository.junction(
                    conn,
                    "topic",
                    TaskJuncture.Junction.SINGULAR,
                    Set.of(Set.of("foo", "bar"), Set.of("qux", "baz")))
            ).containsExactlyInAnyOrder("foo", "bar");

            assertThat(repository.task(conn, "topic", offset + 1).orElseThrow().getState()).isEqualTo(TaskState.READY);
            assertThat(repository.task(conn, "topic", offset + 2).orElseThrow().getState()).isEqualTo(TaskState.READY);
            assertThat(repository.task(conn, "topic", offset + 3).orElseThrow().getState()).isEqualTo(TaskState.SUSPENDED);
        }
    }

    @Test
    public void can_send_heartbeat() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            assertThat(repository.register(conn)).isTrue();

            repository.heartbeat(conn);
        }
    }

    @Test
    public void can_expire_task() throws Exception {
        TaskRepository<Connection, SQLException> other = getRepository("other");
        try (Connection conn = dataSource.getConnection()) {
            assertThat(repository.register(conn)).isTrue();
            assertThat(repository.initialize(conn, "topic")).isTrue();

            assertThat(repository.page(conn, "topic", new TaskRepository.Listing().withState(TaskState.READY), TaskRepository.INCEPTION, Integer.MAX_VALUE)).isEmpty();
            assertThat(repository.page(conn, "topic", TaskRepository.INCEPTION, Integer.MAX_VALUE)).isEmpty();

            Task task = repository.push(conn, "topic", TaskSink.Insertion.APPEND, "foo");
            assertThat(task.getIdentifier()).isEqualTo("foo");
            assertThat(task.getSequence()).isEqualTo(offset + 1);

            assertThat(repository.poll(conn, "topic")).contains(task);

            other.expire(conn, 250, TimeUnit.SECONDS);

            assertThat(repository.task(conn, "topic", offset + 1)).hasValueSatisfying(info -> {
                assertThat(info.getState()).isEqualTo(TaskState.ACTIVE);
                assertThat(info.getDescent()).isEqualTo(TaskState.READY);
                assertThat(info.getSequence()).isEqualTo(offset + 1);
                assertThat(info.getIdentifier()).isEqualTo("foo");
                assertThat(info.getOwner()).contains("owner");
                assertThat(info.getCreated()).isNotNull().isAfter(OffsetDateTime.now().minusMinutes(5));
                assertThat(info.getCompleted()).isNotPresent();
            });

            Thread.sleep(100);

            other.expire(conn, 50, TimeUnit.MILLISECONDS);

            assertThat(repository.task(conn, "topic", offset + 1)).hasValueSatisfying(info -> {
                assertThat(info.getState()).isEqualTo(TaskState.EXPIRED);
                assertThat(info.getDescent()).isEqualTo(TaskState.READY);
                assertThat(info.getSequence()).isEqualTo(offset + 1);
                assertThat(info.getIdentifier()).isEqualTo("foo");
                assertThat(info.getOwner()).contains("owner");
                assertThat(info.getCreated()).isNotNull().isAfter(OffsetDateTime.now().minusMinutes(5));
                assertThat(info.getCompleted()).isNotPresent();
            });
        }
    }

    @Test
    public void can_list_topics() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            assertThat(repository.topics(conn)).isEmpty();

            assertThat(repository.initialize(conn, "topic")).isTrue();

            assertThat(repository.topics(conn)).containsExactly("topic");
        }
    }

    @Test
    public void can_count_total() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            assertThat(repository.initialize(conn, "topic")).isTrue();

            assertThat(repository.count(conn, TaskRepository.Snapshot.TOTAL).get("topic")).isEmpty();

            Task task = repository.push(conn, "topic", TaskSink.Insertion.APPEND, "foo");
            assertThat(task.getIdentifier()).isEqualTo("foo");
            assertThat(task.getSequence()).isEqualTo(offset + 1);

            assertThat(repository.count(conn, TaskRepository.Snapshot.TOTAL).get("topic"))
                .hasSize(1)
                .containsEntry(TaskState.READY, new TaskRepository.Summary(1, offset + 1));
            assertThat(repository.count(conn, TaskRepository.Snapshot.TOTAL, task.getSequence() + 1, Long.MAX_VALUE).get("topic")).isEmpty();
            assertThat(repository.count(
                conn,
                TaskRepository.Snapshot.TOTAL,
                new TaskRepository.Counting().withTopic("topic").withState(TaskState.READY).withIdentifier("foo")
            ).get("topic")).hasSize(1).containsEntry(TaskState.READY, new TaskRepository.Summary(1, offset + 1));
            assertThat(repository.count(
                conn,
                TaskRepository.Snapshot.TOTAL,
                new TaskRepository.Counting().withTopic("topic").withState(TaskState.READY).withIdentifier("foo"),
                task.getSequence() + 1,
                Long.MAX_VALUE
            ).get("topic")).isEmpty();

            Task other = repository.push(conn, "topic", TaskSink.Insertion.REPLACE, "foo");
            assertThat(other.getIdentifier()).isEqualTo("foo");
            assertThat(other.getSequence()).isEqualTo(offset + 2);

            assertThat(repository.count(conn, TaskRepository.Snapshot.TOTAL).get("topic"))
                .hasSize(2)
                .containsEntry(TaskState.READY, new TaskRepository.Summary(1, offset + 2))
                .containsEntry(TaskState.REDUNDANT, new TaskRepository.Summary(1, offset + 1));
            assertThat(repository.count(conn, TaskRepository.Snapshot.TOTAL, other.getSequence() + 1, Long.MAX_VALUE).get("topic")).isEmpty();
            assertThat(repository.count(
                conn,
                TaskRepository.Snapshot.TOTAL,
                new TaskRepository.Counting().withTopic("topic").withState(TaskState.READY).withIdentifier("foo")
            ).get("topic")).hasSize(1).containsEntry(TaskState.READY, new TaskRepository.Summary(1, offset + 2));
            assertThat(repository.count(
                conn,
                TaskRepository.Snapshot.TOTAL,
                new TaskRepository.Counting().withTopic("topic").withState(TaskState.READY).withIdentifier("foo"),
                other.getSequence() + 1,
                Long.MAX_VALUE
            ).get("topic")).isEmpty();

            assertThat(repository.count(
                conn,
                TaskRepository.Snapshot.TOTAL,
                new TaskRepository.Counting().withTopic("other")
            )).isEmpty();
        }
    }

    @Test
    public void can_count_recent() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            assertThat(repository.initialize(conn, "topic")).isTrue();

            assertThat(repository.count(conn, TaskRepository.Snapshot.RECENT).get("topic")).isEmpty();

            Task task = repository.push(conn, "topic", TaskSink.Insertion.APPEND, "foo");
            assertThat(task.getIdentifier()).isEqualTo("foo");
            assertThat(task.getSequence()).isEqualTo(offset + 1);

            assertThat(repository.count(conn, TaskRepository.Snapshot.RECENT).get("topic"))
                .hasSize(1)
                .containsEntry(TaskState.READY, new TaskRepository.Summary(1, offset + 1));
            assertThat(repository.count(conn, TaskRepository.Snapshot.RECENT, task.getSequence() + 1, Long.MAX_VALUE).get("topic")).isEmpty();
            assertThat(repository.count(
                conn,
                TaskRepository.Snapshot.RECENT,
                new TaskRepository.Counting().withTopic("topic").withState(TaskState.READY).withIdentifier("foo")
            ).get("topic")).hasSize(1).containsEntry(TaskState.READY, new TaskRepository.Summary(1, offset + 1));
            assertThat(repository.count(
                conn,
                TaskRepository.Snapshot.RECENT,
                new TaskRepository.Counting().withTopic("topic").withState(TaskState.READY).withIdentifier("foo"),
                task.getSequence() + 1,
                Long.MAX_VALUE
            ).get("topic")).isEmpty();

            Task other = repository.push(conn, "topic", TaskSink.Insertion.REPLACE, "foo");
            assertThat(other.getIdentifier()).isEqualTo("foo");
            assertThat(other.getSequence()).isEqualTo(offset + 2);

            assertThat(repository.count(conn, TaskRepository.Snapshot.RECENT).get("topic"))
                .hasSize(1)
                .containsEntry(TaskState.READY, new TaskRepository.Summary(1, offset + 2));
            assertThat(repository.count(conn, TaskRepository.Snapshot.RECENT, other.getSequence() + 1, Long.MAX_VALUE).get("topic")).isEmpty();
            assertThat(repository.count(
                conn,
                TaskRepository.Snapshot.RECENT,
                new TaskRepository.Counting().withTopic("topic").withState(TaskState.READY).withIdentifier("foo")
            ).get("topic")).hasSize(1).containsEntry(TaskState.READY, new TaskRepository.Summary(1, offset + 2));
            assertThat(repository.count(
                conn,
                TaskRepository.Snapshot.RECENT,
                new TaskRepository.Counting().withTopic("topic").withState(TaskState.READY).withIdentifier("foo"),
                other.getSequence() + 1,
                Long.MAX_VALUE
            ).get("topic")).isEmpty();

            assertThat(repository.count(
                conn,
                TaskRepository.Snapshot.RECENT,
                new TaskRepository.Counting().withTopic("other")
            )).isEmpty();
        }
    }

    @Test
    public void can_count_results() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            assertThat(repository.initialize(conn, "topic")).isTrue();
            assertThat(repository.register(conn)).isTrue();

            assertThat(repository.results(conn).get("topic")).isEmpty();

            Task task = repository.push(conn, "topic", TaskSink.Insertion.APPEND, "foo");
            assertThat(task.getIdentifier()).isEqualTo("foo");
            assertThat(task.getSequence()).isEqualTo(offset + 1);

            assertThat(repository.results(conn).get("topic")).isEmpty();

            Task other = repository.push(conn, "topic", TaskSink.Insertion.REPLACE, "foo");
            assertThat(other.getIdentifier()).isEqualTo("foo");
            assertThat(other.getSequence()).isEqualTo(offset + 2);

            assertThat(repository.results(conn).get("topic")).isEmpty();

            assertThat(repository.poll(conn, "topic")).contains(other);
            repository.complete(conn, "topic", other, TaskDecision.SUCCESS);

            assertThat(repository.results(conn).get("topic"))
                .containsExactly(Map.entry(TaskResult.SUCCESS, 1L));
            assertThat(repository.results(conn, OffsetDateTime.now().minusMinutes(1), OffsetDateTime.now().plusMinutes(1)).get("topic"))
                .containsExactly(Map.entry(TaskResult.SUCCESS, 1L));
            assertThat(repository.results(conn, OffsetDateTime.now().minusMinutes(6), OffsetDateTime.now().minusMinutes(5)).get("topic"))
                .isEmpty();
        }
    }

    @Test
    public void can_destroy_topic() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            assertThat(repository.destroy(conn, "topic")).isFalse();

            assertThat(repository.initialize(conn, "topic")).isTrue();
            assertThat(repository.register(conn)).isTrue();

            Task task = repository.push(conn, "topic", TaskSink.Insertion.APPEND, "foo");
            assertThat(repository.task(conn, "topic", task.getSequence())).isPresent();

            assertThat(repository.destroy(conn, "topic")).isTrue();
            assertThat(repository.destroy(conn, "topic")).isFalse();

            assertThat(repository.task(conn, "topic", task.getSequence())).isEmpty();

            assertThat(repository.initialize(conn, "topic")).isTrue();
        }
    }

    @Test
    public void can_purge_all() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            assertThat(repository.initialize(conn, "topic")).isTrue();
            assertThat(repository.register(conn)).isTrue();

            Task task = repository.push(conn, "topic", TaskSink.Insertion.APPEND, "foo");
            assertThat(repository.task(conn, "topic", task.getSequence())).isPresent();

            repository.purgeAll(conn, "topic");

            assertThat(repository.task(conn, "topic", task.getSequence())).isEmpty();
        }
    }

    @Test
    public void can_purge() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            assertThat(repository.initialize(conn, "topic")).isTrue();
            assertThat(repository.register(conn)).isTrue();

            assertThat(repository.purge(conn, "topic")).isEqualTo(0);

            Task task = repository.push(conn, "topic", TaskSink.Insertion.APPEND, "foo");
            assertThat(repository.task(conn, "topic", task.getSequence())).isPresent();

            assertThat(repository.purge(conn, "topic")).isEqualTo(0);
            assertThat(repository.poll(conn, "topic")).contains(task);
            assertThat(repository.purge(conn, "topic")).isEqualTo(0);
            repository.complete(conn, "topic", task, TaskDecision.SUCCESS);

            assertThat(repository.purge(conn, "topic", TaskState.FILTERED)).isEqualTo(0);
            assertThat(repository.purge(conn, "topic")).isEqualTo(1);
            assertThat(repository.purge(conn, "topic")).isEqualTo(0);

            assertThat(repository.task(conn, "topic", task.getSequence())).isEmpty();
        }
    }

    @Test
    public void can_resolve() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            assertThat(repository.initialize(conn, "topic")).isTrue();

            assertThat(repository.resolve(conn, LocalDate.now(), true)).isEqualTo(TaskRepository.INCEPTION);
            assertThat(repository.resolve(conn, LocalDate.now(), false)).isEqualTo(Long.MAX_VALUE);

            Task task = repository.push(conn, "topic", TaskSink.Insertion.APPEND, "foo");
            TaskInfo info = repository.task(conn, "topic", task.getSequence()).orElseThrow();

            assertThat(repository.resolve(conn, info.getCreated().minusSeconds(1), false)).isEqualTo(task.getSequence());
            assertThat(repository.resolve(conn, info.getCreated(), false)).isEqualTo(task.getSequence());
            assertThat(repository.resolve(conn, info.getCreated().plusSeconds(1), false)).isEqualTo(Long.MAX_VALUE);

            assertThat(repository.resolve(conn, info.getCreated().minusSeconds(1), true)).isEqualTo(TaskRepository.INCEPTION);
            assertThat(repository.resolve(conn, info.getCreated(), true)).isEqualTo(task.getSequence());
            assertThat(repository.resolve(conn, info.getCreated().plusSeconds(1), true)).isEqualTo(task.getSequence());
        }
    }

    @Test
    public void can_read_pretty_view() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            assertThat(repository.initialize(conn, "topic")).isTrue();

            Task task = repository.push(conn, "topic", TaskSink.Insertion.APPEND, new TaskCreation("foo", "input"));

            try (PreparedStatement ps = conn.prepareStatement("SELECT STATE, INPUT FROM TASK_PRETTY WHERE TOPIC = ? AND SEQUENCE = ?")) {
                ps.setString(1, "topic");
                ps.setLong(2, task.getSequence());

                try (ResultSet rs = ps.executeQuery()) {
                    assertThat(rs.next()).isTrue();
                    assertThat(rs.getString("STATE")).isEqualTo(TaskState.READY.name());
                    assertThat(rs.getString("INPUT")).isEqualTo("input");
                    assertThat(rs.next()).isFalse();
                }
            }
        }
    }
}
