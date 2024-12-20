package is.yarr.qilletni.lib.postgres.exceptions;

import is.yarr.qilletni.api.exceptions.QilletniException;

public class InvalidPreparedStatementType extends QilletniException {

    public InvalidPreparedStatementType() {
        super();
    }

    public InvalidPreparedStatementType(String message) {
        super(message);
    }

    public InvalidPreparedStatementType(Throwable cause) {
        super(cause);
    }
}
