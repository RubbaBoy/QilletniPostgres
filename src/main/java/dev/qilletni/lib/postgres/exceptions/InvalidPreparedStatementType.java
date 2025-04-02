package dev.qilletni.lib.postgres.exceptions;

import dev.qilletni.api.exceptions.QilletniException;

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
