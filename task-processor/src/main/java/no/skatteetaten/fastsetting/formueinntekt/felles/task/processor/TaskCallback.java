package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor;

public interface TaskCallback<TRANSACTION, EXCEPTION extends Exception> {

    void accept(TaskCompletion<TRANSACTION, EXCEPTION> completion) throws EXCEPTION;
}
