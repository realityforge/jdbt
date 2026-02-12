package org.realityforge.jdbt.runtime;

public final class RuntimeExecutionException extends RuntimeException {
    public RuntimeExecutionException(final String message) {
        super(message);
    }

    public RuntimeExecutionException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
