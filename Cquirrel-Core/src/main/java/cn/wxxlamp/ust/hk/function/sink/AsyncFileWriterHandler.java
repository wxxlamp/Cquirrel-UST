package cn.wxxlamp.ust.hk.function.sink;

/**
 * Singleton handler for managing the AsyncFileWriter instance.
 * Ensures only one instance of AsyncFileWriter is created and used throughout the application.
 *
 * @author wxx
 * @version 2025-08-06 23:10
 */
public class AsyncFileWriterHandler {

    private static volatile AsyncFileWriter asyncFileWriterHandler;

    /**
     * Gets the singleton instance of AsyncFileWriter.
     * Uses double-checked locking to ensure thread safety.
     *
     * @return the singleton AsyncFileWriter instance
     */
    public static AsyncFileWriter getAsyncFileWriter() {
        if (asyncFileWriterHandler == null) {
            synchronized (AsyncFileWriterHandler.class) {
                if (asyncFileWriterHandler == null) {
                    asyncFileWriterHandler = AsyncFileWriter.create();
                }
            }
        }
        return asyncFileWriterHandler;
    }
}
