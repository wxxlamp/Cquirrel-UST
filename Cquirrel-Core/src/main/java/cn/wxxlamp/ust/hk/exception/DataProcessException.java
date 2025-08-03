package cn.wxxlamp.ust.hk.exception;

import java.io.Serial;

/**
 * @author wxx
 * @version 2025-08-03 17:04
 */
public class DataProcessException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    public DataProcessException() {
        super();
    }

    public DataProcessException(String message) {
        super(message);
    }

    public DataProcessException(String message, Throwable cause) {
        super(message, cause);
    }

    public DataProcessException(Throwable cause) {
        super(cause);
    }
}
