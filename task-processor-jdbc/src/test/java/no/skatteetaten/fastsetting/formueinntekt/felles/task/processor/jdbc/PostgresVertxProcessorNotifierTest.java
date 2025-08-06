package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.jdbc;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.vertx.core.Vertx;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.pubsub.PgSubscriber;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.jdbc.JdbcTaskRepository;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.jdbc.PostgresTaskRepository;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.ProcessorNotifier;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.TaskProcessor;
import org.junit.Before;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

@Category(PostgreSQLContainer.class) // Will fail due to outdated Netty version of PostgresNg driver.
public class PostgresVertxProcessorNotifierTest extends AbstractJdbcProcessorNotifierTest<HikariDataSource> {

    @Rule
    public JdbcDatabaseContainer<?> container = new PostgreSQLContainer<>("postgres:11");

    private Vertx vertx;

    private PgSubscriber subscriber;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        vertx = Vertx.vertx();
        subscriber = PgSubscriber.subscriber(vertx, new PgConnectOptions()
            .setPort(container.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT))
            .setHost(container.getHost())
            .setDatabase(container.getDatabaseName())
            .setUser(container.getUsername())
            .setPassword(container.getPassword())
        );
        subscriber.connect().toCompletionStage().toCompletableFuture().get();
    }

    @Override
    public void tearDown() throws Exception {
        subscriber.close().toCompletionStage().toCompletableFuture().get();
        vertx.close().toCompletionStage().toCompletableFuture().get();

        super.tearDown();
    }

    @Override
    protected HikariDataSource dataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(container.getJdbcUrl());
        config.setUsername(container.getUsername());
        config.setPassword(container.getPassword());
        return new HikariDataSource(config);
    }

    @Override
    protected void shutdown(HikariDataSource dataSource) { }

    @Override
    protected ProcessorNotifier processorNotifier(HikariDataSource dataSource, Collection<TaskProcessor> processors) {
        return new PostgresVertxProcessorNotifier(
            subscriber,
            () -> { },
            processors,
            new DefaultTaskChangeHandler(1, TimeUnit.SECONDS)
        );
    }

    @Override
    protected JdbcTaskRepository taskRepository(String owner) {
        return new PostgresTaskRepository(false, owner);
    }
}
