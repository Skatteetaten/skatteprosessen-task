Generic task processing
=======================

This library implements a minimal framework for handling an ordered queue of tasks that is both suitable for stream and batch processing. Tasks queues are identified by a *topic* name where a task is represented by any `String` value to identify a specific (and possibly repeated) unit of work in addition to an optional `String` payload. New tasks are represented as instances of `TaskCreation`.

`Task`s are created by pushing `TaskCreation`s to a topic by using a `TaskSink`. When pushing a task, it can be specified if pending tasks with the same identifier should be rendered as redundant and no longer be processed (`TaskSink.Insertion.REPLACE`), should only set non-processed tasks as redundant (`TaskSink.Insertion.SUPERSEDE`), or if the new task should be added to the end of the queue in addition to any preexisting tasks (`TaskSink.Insertion.APPEND`). Alternatively, all previous, non-active tasks can be deleted (`TaskSink.Insertion.DELETE`).

To process a task, it is polled from a `TaskSource` and then completed by determining a `TaskDecision` for each task. A task decision carries an optional completion message or exception which is stored as a result. As a requirement, a decision also needs to determine a `TaskResult` which can be any of:

- `SUCCESS`: A task is marked as successful.
- `SUSPENSION`: A task cannot currently be completed but should be considered later.
- `FILTER`: A task was found to be irrelevant.
- `FAILURE`: A task failed during execution.

Tasks are processed in the order they were committed to a sink, what is reflected by the sequence number that is assigned to each task. Note that the sequence number does not reflect an absolute processing order if tasks are pushed by multiple concurrent producers and/or are polled by multiple concurrent consumers. Tasks are however guaranteed to be sequenced per individual producer or consumer. The order can be reversed by defining `TaskSource.Order.LAST_IN_FIRST_OUT`. Individual tasks can also be excluded from polling by setting a `TaskSource.Condition` where tasks can be excluded until any other task for the same identifier is processed (`TaskSource.Condition.SINGULAR_BY_IDENTIFIER`), is processed without error (`TaskSource.Condition.SINGULAR_BY_IDENTIFIER_SUSPEND_ON_FAILURE`) or is successfully processed (`TaskSource.Condition.SINGULAR_BY_IDENTIFIER_SUSPEND_UNTIL_SUCCESS`).

All API and data representation is defined in the *task-api* artifact.

Task lifecycle
--------------

As described, a task is pushed to a `TaskSink` and later polled and completed by a `TaskSource`. Each interaction with such a storage system is normally happening in the scope of a transaction, for example a JDBC `Connection` when the store is implemented by a relational database. It is also possible to specify an exception type that is known to the storage system, for example an `SQLException` in the example of JDBC.

In order to reprocess a task, a task can either be reset or a new task with the same identifier and payload can be pushed to the same `TaskSink`. A `TaskReviver` offer functionality to do either where recreated tasks are also marked as being recreated. Recreation should be prefered over resetting if previous results are needed for further processing.

Task repository
---------------

A `TaskRepository` represents a combined `TaskSource`, `TaskSink` and `TaskReviver`. In addition, it offers API for navigating pages of tasks for given topics where richer metadata is provided by `TaskInfo` instances. A `TaskRepository` requires explicit registration before usage and initialization of each topic. Furthermore, it offers functionality for sending a heartbeat and to expire tasks held by other repositories, possibly by other JVMs.

A JDBC-based repository is included in the *task-jdbc* artifact. For using ANSI SQL, a `JdbcTaskRepository` can be used but there are also derived implementations for Oracle and PostgreSQL available. Any `JdbcTaskRepository` can be used in a concurrent mode that allows for pushing, polling and completing tasks concurrently from multiple threads or VMs. In order to use a repository concurrently, the underlying database must support MVCC with *SELECT ... FOR UPDATE SKIP LOCKED* semantics.

The database is organized within multiple tables:

- *TASK* contains all registered tasks where the state of the task is encoded in an ordinal value of a task's `TaskState`. This is done to allow for range-selects on the table where for example all completed tasks are between two and five. By fixing the statement, we can make better use of query caches and simplify the databases use of its indices.
- *TASK_PRETTY* is a view on *TASK* that eases manual searches in the database by translating ordinal states to their names. If *no.skatteetaten.fastsetting.formueinntekt.felles.task.jdbc.pretty* is set to false, the pretified view is not generated. When using Hyper SQL, a view with additional, texified columns for input and output is created to ease the use of Hyper SQL's built-in database manager GUI.
- *TASK_TOPIC* contains a list of registered topics. The table is used to disallow registering non-existing or misspelled topics.
- *TASK_OWNER* contains a list of owners for any given task which is also used as a measure to decide if a task has expired by considering the owners most recent heart beat.

The task table is automatically partitioned by topic if Oracle or Postgres is used. Partitioning can be disabled by setting *no.skatteetaten.fastsetting.formueinntekt.felles.task.jdbc.partitioned* to false.

In order to clear tasks, it is possible to purge tasks that are lower then a given sequence number from a repository. This allows to clear out tasks after they are no longer needed. Finally, it is possible to resolve any given `OffsetDateTime` to the first sequence number that is assigned after it. Using this mechanism, it is possible to select tasks within date ranges rather than by a sequence number. Specified dates are always synchronized with the database clock to avoid accidental mismatches.  

The total number of tasks can be counted, also limited to any given sequence range. Counting can also consider the most recent task for any given identifier.

A task repository also implements the `TaskJunctor` interface which allows to start suspended tasks only if tasks for multiple identifiers were added to the repository. This can be conditioned on all other relevant identifiers defining tasks in suspended state (`TaskJuncture.Junction.SINGULAR`), or without such a condition where any state is considered (`TaskJuncture.Junction.ETERNAL`).  

Simple example
--------------

In order to push, poll and complete a tasks for identifiers `identifier`, the following example code demonstrates the use:

```java
TaskRepository<TRANSACTION, RuntimeException> repository = ...
Supplier<TRANSACTION> transaction = ...;

repository.push(transaction.get(), "topic", TaskSource.Insertion.APPEND, "identifier");

// Typically the following code is run in another thread.
Optional<Task> task = repository.poll(transaction.get(), "topic");

// After polling a task, it is normally processed in a meaningful manner.
repository.complete(transaction.get(), "topic", task.orElseThrow(), TaskDecision.SUCCESS);
```

By pushing a list of identifiers and by specifying a polling size as an additional argument, batches of tasks can be processed within a singular transaction. Normally, it is however recommended to poll and complete tasks in using a `TaskProcessor` which are described in the following section.

Task processing
---------------

While pushing tasks is typically straight-forward, processing tasks can be more challenging as it requires two steps (polling and completing) and should also be implemented within a loop that repeatedly checks the task queue for new work. To simplify the implementation, the *task-processor* artifact contains several convenience implementations to ease implementing reliable and concurrent processing.

A poll loop is implemented by a `TaskProcessor` where `TaskManager` is a default implementation. The task manager implementation is polling from a `TaskSource` and dispatches all found tasks to a `TaskConsumer`. The latter consumer can complete the supplied tasks by invoking the provided `TaskCallback` or fail the entire batch by providing an exception to the provided `onFailure` consumer. It is possible to complete a provided batch from any thread, but it is the responsibility of the consumer to block the polling thread if no additional tasks should be polled. A task manager can be started and stopped. If a running task manager is reaching the end of a queue, it will pause for the given interval before reattempting to poll from a queue unless the resume method is invoked where the queue will be checked for new tasks immediately if the manager is currently in hibernation.

Given a trivial `TaskConsumer`, a `TaskManager` can be executed as follows for a given topic:

```java
ExecutorService service = Executors.newSingleThreadExecutor(); // the executor to run task processing
TaskRepository<TRANSACTION, RuntimeException> repository = ... // used for polling and completing tasks
TaskDispatcher<TRANSACTION, EXCEPTION> dispatcher = ... // responsible for creating a TRANSACTION instance
TaskProcessor processor = new TaskManager<>(
  "topic",
  service,
  repository,
  dispatcher,
  TaskLimiter.noop(), // no limitation of concurrently processed tasks
  TaskConsumer.simple(task -> TaskDecision.SUCCESS).toFactory(), // trivial consumer that succeeds all tasks
  TaskErrorDecorator.noop(), // does not do anything if tasks are explicitly failed
  TaskListener.onFatal(throwable -> throwable.printStackTrace()), // prints fatal errors
  1_000, // processes a maximum of 1000 tasks at any time
  100, TimeUnit.SECONDS // pauses 100 seconds when no more tasks are found.
);
```

Instead of implementing a `TaskConsumer` directly, tasks can be polled from a queue into a buffer by using a
`BufferingTaskHandlerFactory` as a decorator for another `TaskConsumer`. The buffering task consumer factory is delegating all of its tasks to a pool of workers and keeps a specified amount of tasks ready for processing to reduce the time for processing a next batch when a worker becomes available:

```java
ExecutorService service = Executors.newSingleThreadExecutor(); // the executor to run task processing
Function<String, TaskConsumer<TRANSACTION, EXCEPTION>> factory = new BufferingTaskHandlerFactory<>(
  service,
  false, // determines if buffered tasks should be failed when a consumer is closed
  5, // the length of the queue
  10, // the amount of workers for parallel processing
  1, TimeUnit.SECONDS, // the amount of time to block on an empty queue to check if a consumer is closed
  TaskHandler.simple(task -> TaskDecision.SUCCESS).toFactory() // the actual consumer
);
```

Finally, tasks can be processed as a series of composite steps by using a `CompositeTaskHandlerFactory`. For using this factory, all steps are registered with an identity where subsequent steps can declare to be dependent on previous steps. If multiple steps are not dependent on one another, they are run in parallel once their dependant steps have completed successfully. If any dependant step fails a task, it will be marked as failed and not be provided to subsequent steps, unless the `continue` flag is explicitly set in a `TaskDecision`. If a step fails all tasks, subsequent steps will no longer be invoked. It is recommended to use a `ForkJoinPool` as an executor to amplify the use of parallel streams within per-task steps. If two parallel steps determine a different result for a task, the actual result is determined by the following priority order from top to bottom:

- `FAILURE`
- `SUSPENSION`
- `FILTER`
- `SUCCESS`

If data is retrieved or generated in one of these steps, it can be registered in a `TaskSupplement` and read from other, subsequent steps. A task supplement is generated for any batch of tasks and preserved until completion.

As an example, the following composite factory composes two steps where the second step depends on the first. 

```java
ExecutorService service = Executors.newSingleThreadExecutor(); // the executor to run task processing
Function<String, TaskConsumer<TRANSACTION, EXCEPTION>> factory = new CompositeTaskHandlerFactory<>(
  service,
  false, // determines if pending tasks should be failed when a consumer is closed
  10, // the amount of workers for parallel processing
  TaskContext.simple().toFactory()
).withSimpleSingularSyncStep(
  "step1", // The name of the first step
  (task, supplement) -> { System.out.println("Running step 1 for " + task); }
).withSimpleSingularSyncStep(
  "step2", // The name of the second step
  (task, supplement) -> { System.out.println("Running step 2 for " + task); },
  "step1" // Determines that this second step depends on the first step
);
```

The composite implementation also makes use of a `TaskContext` which allows for post-processing tasks that have been handed through a series of steps. For convenience, the `CompositeTaskHandlerFactory` allows for the combination of multiple such contexts. On completing a series of steps, the context is then responsible to make any final adjustments to the resulting tasks and to contribute to the committing transaction.

If the state of task processing should survive a JVM restart, the *task-processor-jdbc* module offers wrappers for persisting the state. The `initialize` and `shutdown` methods will then materialize the persisted state which is stored in a database.
