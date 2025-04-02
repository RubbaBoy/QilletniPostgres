package dev.qilletni.lib.postgres;

import dev.qilletni.api.lang.types.EntityType;
import dev.qilletni.api.lang.types.JavaType;
import dev.qilletni.api.lang.types.QilletniType;
import dev.qilletni.api.lib.annotations.BeforeAnyInvocation;
import dev.qilletni.api.lib.annotations.NativeOn;

import java.sql.PreparedStatement;
import java.sql.SQLException;

@NativeOn("PreparedStatement")
public class PreparedStatementFunctions {

    private PreparedStatement preparedStatement;

    @BeforeAnyInvocation
    public void setupPreparedStatement(EntityType entityType) {
        preparedStatement = entityType.getEntityScope().<JavaType>lookup("_preparedStatement").getValue().getReference(PreparedStatement.class);
    }
    
    public boolean setParam(EntityType entityType, int index, QilletniType value) {
        try {
            preparedStatement.setObject(index, DatabaseTypeUtility.fromQilletniToNativeJava(value));
            return true;
        } catch (SQLException e) {
            return false;
        }
    }
    
    public boolean close(EntityType entityType) {
        try {
            preparedStatement.close();
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

}
