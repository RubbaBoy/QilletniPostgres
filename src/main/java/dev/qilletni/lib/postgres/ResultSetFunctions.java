package dev.qilletni.lib.postgres;

import dev.qilletni.api.lang.types.EntityType;
import dev.qilletni.api.lang.types.IntType;
import dev.qilletni.api.lang.types.JavaType;
import dev.qilletni.api.lang.types.QilletniType;
import dev.qilletni.api.lang.types.StringType;
import dev.qilletni.api.lib.annotations.BeforeAnyInvocation;
import dev.qilletni.api.lib.annotations.NativeOn;
import dev.qilletni.lib.postgres.exceptions.DatabaseException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;

@NativeOn("ResultSet")
public class ResultSetFunctions {
    
    private ResultSet resultSet;
    private Statement statement;
    
    @BeforeAnyInvocation
    public void setupResultSet(EntityType entityType) {
        this.resultSet = entityType.getEntityScope().<JavaType>lookup("_resultSet").getValue().getReference(ResultSet.class);
        this.statement = (Statement) entityType.getEntityScope().<JavaType>lookup("_statement").getValue().getReference(Optional.class).orElse(null);
    }
    
    public boolean hasNext(EntityType entityType) {
        try {
            return resultSet.next();
        } catch (SQLException e) {
            return false;
        }
    }
    
    public Object getValue(EntityType entityType, QilletniType column) {
        try {
            if (column instanceof StringType columnName) {
                return resultSet.getObject(columnName.getValue());
            } else if (column instanceof IntType columnIndex) {
                return resultSet.getObject(((int) columnIndex.getValue()) + 1);
            }

            return null;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }
    
    public void close(EntityType entityType) {
        try {
            resultSet.close();
            
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }
    
}
