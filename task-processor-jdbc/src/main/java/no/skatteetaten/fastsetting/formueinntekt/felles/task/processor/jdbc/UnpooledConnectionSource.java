package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

@FunctionalInterface
public interface UnpooledConnectionSource<CONNECTION extends Connection> {

    CONNECTION get() throws SQLException;
}
