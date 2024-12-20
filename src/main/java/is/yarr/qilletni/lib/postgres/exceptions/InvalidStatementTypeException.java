package is.yarr.qilletni.lib.postgres.exceptions;

import is.yarr.qilletni.api.exceptions.QilletniException;

public class InvalidStatementTypeException extends QilletniException {

    public InvalidStatementTypeException() {
        super();
    }

    public InvalidStatementTypeException(String message) {
        super(message);
    }

    public InvalidStatementTypeException(Throwable cause) {
        super(cause);
    }
}
