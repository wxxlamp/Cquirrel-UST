package cn.wxxlamp.ust.hk.function.sink;

/**
 * @author wxx
 * @version 2025-08-06 23:10
 */
public class AsyncFileWriterHandler {

    private static volatile AsyncFileWriter asyncFileWriterHandler;

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
