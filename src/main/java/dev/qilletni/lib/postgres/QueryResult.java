package dev.qilletni.lib.postgres;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * An object to wrap both a {@link ResultSet} and optionally (if not a prepared statement) a {@link Statement} to close
 * them both at once.
 * 
 * @param resultSet The result set
 * @param statement The statement, if any
 */
public record QueryResult(ResultSet resultSet, Statement statement) implements AutoCloseable {
    
    public QueryResult(ResultSet resultSet) {
        this(resultSet, null);
    }
    
    @Override
    public void close() throws SQLException {
        if (resultSet != null) resultSet.close();
        if (statement != null) statement.close();
    }
}
