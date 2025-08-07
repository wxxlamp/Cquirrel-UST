package cn.wxxlamp.ust.hk.exception;

import java.io.Serial;

/**
 * Base exception class for data processing errors in the TPC-H Q3 application.
 * This is the parent class for all data processing related exceptions.
 *
 * @author wxx
 * @version 2025-08-03 17:04
 */
public class DataProcessException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * Default constructor
     */
    public DataProcessException() {
        super();
    }

    /**
     * Constructor with message
     * @param message The exception message
     */
    public DataProcessException(String message) {
        super(message);
    }

    /**
     * Constructor with message and cause
     * @param message The exception message
     * @param cause The cause of the exception
     */
    public DataProcessException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructor with cause
     * @param cause The cause of the exception
     */
    public DataProcessException(Throwable cause) {
        super(cause);
    }
}
