package is.yarr.qilletni.lib.postgres;

import is.yarr.qilletni.api.lang.types.AnyType;
import is.yarr.qilletni.api.lang.types.BooleanType;
import is.yarr.qilletni.api.lang.types.DoubleType;
import is.yarr.qilletni.api.lang.types.IntType;
import is.yarr.qilletni.api.lang.types.QilletniType;
import is.yarr.qilletni.api.lang.types.StringType;
import is.yarr.qilletni.lib.postgres.exceptions.InvalidPreparedStatementType;

public class DatabaseTypeUtility {

    public static Object fromQilletniToNativeJava(QilletniType qilletniType) {
        if (!(qilletniType instanceof AnyType anyType)) {
            throw new InvalidPreparedStatementType("Invalid type for PreparedStatement: %s".formatted(qilletniType.getTypeClass().getTypeName()));
        }

        return switch (anyType) {
            case BooleanType booleanType -> booleanType.getValue();
            case DoubleType doubleType -> doubleType.getValue();
            case IntType intType -> intType.getValue();
            case StringType stringType -> stringType.getValue();
//                    case JavaType javaType -> {} // TODO: Maybe store memory locations here? Only valid for certain instances of course
//                    case SongType songType -> {} // TODO: Store song by ID/Provider?
            default -> throw new InvalidPreparedStatementType("Invalid type for PreparedStatement: %s".formatted(qilletniType.getTypeClass().getTypeName()));
        };
    }
    
}
