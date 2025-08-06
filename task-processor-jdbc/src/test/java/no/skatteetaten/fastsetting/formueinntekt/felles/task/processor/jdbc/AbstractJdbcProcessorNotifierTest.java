package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;

import javax.sql.DataSource;
import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskSink;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.jdbc.JdbcTaskRepository;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.ProcessorNotifier;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.TaskProcessor;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public abstract class AbstractJdbcProcessorNotifierTest<T extends DataSource> {

    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    private T dataSource;

    @Mock
    private TaskProcessor processor, other;

    @Before
    public void setUp() throws Exception {
        dataSource = dataSource();

        try (Connection conn = dataSource.getConnection()) {
            Liquibase liquibase = new Liquibase(JdbcTaskRepository.CHANGE_LOG,
                new ClassLoaderResourceAccessor(),
                DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(conn)));
            liquibase.update(new Contexts());
        }

        try (Connection conn = dataSource.getConnection()) {
            Liquibase liquibase = new Liquibase(JdbcPersistentTaskProcessor.CHANGE_LOG,
                new ClassLoaderResourceAccessor(),
                DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(conn)));
            liquibase.update(new Contexts());
        }

        when(processor.getTopic()).thenReturn("topic");
        when(other.getTopic()).thenReturn("other");
    }


    @After
    public void tearDown() throws Exception {
        shutdown(dataSource);
    }

    protected abstract T dataSource() throws SQLException;

    protected abstract void shutdown(T dataSource) throws SQLException;

    protected abstract ProcessorNotifier processorNotifier(T dataSource, Collection<TaskProcessor> processors) throws SQLException;

    protected abstract JdbcTaskRepository taskRepository(String owner);

    @Test
    public void can_notify_topic() throws Exception {
        ProcessorNotifier notifier = processorNotifier(dataSource, Arrays.asList(processor, other));
        assertThat(notifier.isActive()).isFalse();
        notifier.start();
        try {
            assertThat(notifier.isActive()).isTrue();
            JdbcTaskRepository repository = taskRepository("owner");
            try (Connection conn = dataSource.getConnection()) {
                assertThat(repository.register(conn)).isTrue();
                assertThat(repository.initialize(conn, "topic")).isTrue();
                repository.push(conn, "topic", TaskSink.Insertion.APPEND, "identifier");
            }
            verify(processor, timeout(2500)).resume();
        } finally {
            notifier.stop();
        }
        assertThat(notifier.isActive()).isFalse();
        verify(other, never()).resume();
    }

    @Test
    public void does_not_notify_topic_after_stop() throws Exception {
        ProcessorNotifier notifier = processorNotifier(dataSource, Arrays.asList(processor, other));
        assertThat(notifier.isActive()).isFalse();
        notifier.start();
        try {
            assertThat(notifier.isActive()).isTrue();
        } finally {
            notifier.stop();
        }
        JdbcTaskRepository repository = taskRepository("owner");
        try (Connection conn = dataSource.getConnection()) {
            assertThat(repository.register(conn)).isTrue();
            assertThat(repository.initialize(conn, "topic")).isTrue();
            repository.push(conn, "topic", TaskSink.Insertion.APPEND, "identifier");
        }
        assertThat(notifier.isActive()).isFalse();
        verify(processor, never()).resume();
        verify(other, never()).resume();
    }
}
