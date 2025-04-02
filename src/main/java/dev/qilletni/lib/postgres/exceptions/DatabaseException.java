package dev.qilletni.lib.postgres.exceptions;

import dev.qilletni.api.exceptions.QilletniException;

public class DatabaseException extends QilletniException {

    public DatabaseException() {
        super();
    }

    public DatabaseException(String message) {
        super(message);
    }

    public DatabaseException(Throwable cause) {
        super(cause);
    }
}
