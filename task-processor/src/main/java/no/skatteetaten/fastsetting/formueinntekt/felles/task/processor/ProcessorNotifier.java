package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor;

public interface ProcessorNotifier {

    boolean isActive();

    boolean start();

    boolean stop();
}
