package is.yarr.qilletni.lib.postgres;

import is.yarr.qilletni.api.lang.types.EntityType;
import is.yarr.qilletni.api.lang.types.JavaType;
import is.yarr.qilletni.api.lang.types.QilletniType;
import is.yarr.qilletni.api.lib.annotations.BeforeAnyInvocation;
import is.yarr.qilletni.api.lib.annotations.NativeOn;

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
