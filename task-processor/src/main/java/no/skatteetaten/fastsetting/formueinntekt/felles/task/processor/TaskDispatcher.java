package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor;

import java.util.function.Supplier;

@FunctionalInterface
public interface TaskDispatcher<TRANSACTION, EXCEPTION extends Exception> {

    default void accept(TransactionConsumer<TRANSACTION, EXCEPTION> consumer) throws EXCEPTION {
        apply(transaction -> {
            consumer.accept(transaction);
            return null;
        });
    }

    <PAYLOAD> PAYLOAD apply(TransactionFunction<TRANSACTION, EXCEPTION, PAYLOAD> function) throws EXCEPTION;

    static <TRANSACTION, EXCEPTION extends Exception> TaskDispatcher<TRANSACTION, EXCEPTION> simple(Supplier<TRANSACTION> supplier) {
        return new TaskDispatcher<>() {
            @Override
            public <PAYLOAD> PAYLOAD apply(TransactionFunction<TRANSACTION, EXCEPTION, PAYLOAD> function) throws EXCEPTION {
                return function.apply(supplier.get());
            }
        };
    }

    @FunctionalInterface
    interface TransactionConsumer<TRANSACTION, EXCEPTION extends Exception> {

        void accept(TRANSACTION transaction) throws EXCEPTION;
    }

    @FunctionalInterface
    interface TransactionFunction<TRANSACTION, EXCEPTION extends Exception, PAYLOAD> {

        PAYLOAD apply(TRANSACTION transaction) throws EXCEPTION;
    }
}
