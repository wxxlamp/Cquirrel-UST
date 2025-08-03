package cn.wxxlamp.ust.hk.exception;

import java.io.Serial;

/**
 * @author wxx
 * @version 2025-08-03 17:05
 */
public class DataFormatException extends DataProcessException {

    @Serial
    private static final long serialVersionUID = 11L;

    public DataFormatException() {
        super();
    }

    public DataFormatException(String message) {
        super(message);
    }

    public DataFormatException(String message, Throwable cause) {
        super(message, cause);
    }

    public DataFormatException(Throwable cause) {
        super(cause);
    }
}
