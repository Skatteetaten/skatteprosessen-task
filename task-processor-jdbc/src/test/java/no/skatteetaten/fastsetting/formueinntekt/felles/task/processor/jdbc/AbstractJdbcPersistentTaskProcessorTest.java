package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;
import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.TaskProcessor;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public abstract class AbstractJdbcPersistentTaskProcessorTest<T extends DataSource> {

    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    private T dataSource;

    @Mock
    private TaskProcessor delegate;

    private JdbcPersistentTaskProcessor processor;

    @Before
    public void setUp() throws Exception {
        dataSource = dataSource();

        try (Connection conn = dataSource.getConnection()) {
            Liquibase liquibase = new Liquibase(JdbcPersistentTaskProcessor.CHANGE_LOG,
                new ClassLoaderResourceAccessor(),
                DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(conn)));
            liquibase.update(new Contexts());
        }

        processor = processor(delegate, dataSource);

        when(delegate.getTopic()).thenReturn("foo");
    }

    @After
    public void tearDown() throws Exception {
        shutdown(dataSource);
    }

    protected abstract T dataSource() throws SQLException;

    protected abstract void shutdown(T dataSource) throws SQLException;

    protected abstract JdbcPersistentTaskProcessor processor(TaskProcessor delegate, T dataSource);

    @Test
    public void fetch_no_previous() {
        assertThat(processor.findActivation()).isNotPresent();
    }

    @Test
    public void fetch_active() {
        processor.write(TaskProcessor.Activation.ACTIVE);
        assertThat(processor.findActivation()).contains(TaskProcessor.Activation.ACTIVE);
    }

    @Test
    public void fetch_inactive() {
        processor.write(TaskProcessor.Activation.INACTIVE);
        assertThat(processor.findActivation()).contains(TaskProcessor.Activation.INACTIVE);
    }

    @Test
    public void start_delegate_run() throws Exception {
        when(delegate.start(500, TimeUnit.MILLISECONDS)).thenReturn(true);
        assertThat(processor.start(500, TimeUnit.MILLISECONDS)).isTrue();
        assertThat(processor.findActivation()).contains(TaskProcessor.Activation.ACTIVE);
    }

    @Test
    public void start_delegate_no_run() throws Exception {
        when(delegate.start(500, TimeUnit.MILLISECONDS)).thenReturn(false);
        assertThat(processor.start(500, TimeUnit.MILLISECONDS)).isFalse();
        assertThat(processor.findActivation()).isNotPresent();
    }

    @Test
    public void update_via_stop() throws Exception {
        when(delegate.stop(100, TimeUnit.MILLISECONDS)).thenReturn(true);
        assertThat(processor.stop(100, TimeUnit.MILLISECONDS)).isTrue();
        assertThat(processor.findActivation()).contains(TaskProcessor.Activation.INACTIVE);
    }

    @Test
    public void update_via_stop_already_stopped() throws Exception {
        when(delegate.stop(100, TimeUnit.MILLISECONDS)).thenReturn(false);
        assertThat(processor.stop(100, TimeUnit.MILLISECONDS)).isFalse();
        assertThat(processor.findActivation()).isNotPresent();
    }

    @Test
    public void no_action_on_resume() {
        processor.resume();
        assertThat(processor.findActivation()).isEmpty();
        verify(delegate).resume();
    }

    @Test
    public void start_conditional_active() throws Exception {
        when(delegate.initialize(500, TimeUnit.MILLISECONDS)).thenReturn(true);
        processor.write(TaskProcessor.Activation.ACTIVE);
        assertThat(processor.initialize(500, TimeUnit.MILLISECONDS)).isTrue();
        verify(delegate).initialize(500, TimeUnit.MILLISECONDS);
    }

    @Test
    public void start_conditional_active_not_handled_by_delegate() throws Exception {
        processor.write(TaskProcessor.Activation.ACTIVE);
        assertThat(processor.initialize(500, TimeUnit.MILLISECONDS)).isFalse();
        verify(delegate).initialize(500, TimeUnit.MILLISECONDS);
    }

    @Test
    public void start_conditional_inactive() throws Exception {
        processor.write(TaskProcessor.Activation.INACTIVE);
        assertThat(processor.initialize(500, TimeUnit.MILLISECONDS)).isTrue();
        verify(delegate, never()).start(500, TimeUnit.MILLISECONDS);
    }

    @Test
    public void start_no_previous_state() throws Exception {
        assertThat(processor.initialize(500, TimeUnit.MILLISECONDS)).isFalse();
        verify(delegate, never()).start(500, TimeUnit.MILLISECONDS);
    }

    @Test
    public void shutdown_no_activation_change() throws Exception {
        processor.write(TaskProcessor.Activation.ACTIVE);
        when(delegate.shutdown(500, TimeUnit.MILLISECONDS)).thenReturn(true);
        assertThat(processor.shutdown(500, TimeUnit.MILLISECONDS)).isTrue();
        verify(delegate).shutdown(500, TimeUnit.MILLISECONDS);
        assertThat(processor.findActivation()).contains(TaskProcessor.Activation.ACTIVE);
    }
}
