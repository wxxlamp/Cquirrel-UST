package cn.wxxlamp.ust.hk.exception;

import java.io.Serial;

/**
 * Exception thrown when there is an error in data format during processing.
 * This exception is typically thrown when parsing or validating data fields fails.
 *
 * @author wxx
 * @version 2025-08-03 17:05
 */
public class DataFormatException extends DataProcessException {

    @Serial
    private static final long serialVersionUID = 11L;

    /**
     * Default constructor
     */
    public DataFormatException() {
        super();
    }

    /**
     * Constructor with message
     * @param message The exception message
     */
    public DataFormatException(String message) {
        super(message);
    }

    /**
     * Constructor with message and cause
     * @param message The exception message
     * @param cause The cause of the exception
     */
    public DataFormatException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructor with cause
     * @param cause The cause of the exception
     */
    public DataFormatException(Throwable cause) {
        super(cause);
    }
}
