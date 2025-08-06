package cn.wxxlamp.ust.hk.function.sink;

import cn.wxxlamp.ust.hk.constant.FilePathConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * print the results to file using async thread
 * @author wxx
 * @version 2025-08-06 22:22
 */
public class AsyncFileWriter implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(AsyncFileWriter.class);
    private final BlockingQueue<String> dataQueue;
    private final String outputFilePath;
    private final AtomicBoolean isRunning;
    private Thread writerThread;
    private PrintWriter printWriter;

    // print header
    private static final String HEADER = "l_orderkey, o_orderdate, o_shippriority, revenue";

    public AsyncFileWriter() {
        this.dataQueue = new LinkedBlockingQueue<>(10000); // 队列容量限制
        this.outputFilePath = FilePathConstants.OUTPUT_FILE_PATH;
        this.isRunning = new AtomicBoolean(false);
    }

    public static AsyncFileWriter create() {
        return new AsyncFileWriter();
    }

    /**
     * 启动异步写入线程
     */
    public synchronized void start() throws IOException {
        if (isRunning.get()) {
            LOG.warn("异步写入线程已启动");
            return;
        }

        // 初始化文件写入器（覆盖模式）
        printWriter = new PrintWriter(new FileWriter(outputFilePath, false));
        // 写入表头
        printWriter.println(HEADER);
        printWriter.flush();

        isRunning.set(true);
        writerThread = new Thread(this::writeLoop, "Async-File-Writer");
        writerThread.start();
        LOG.info("异步文件写入线程已启动，输出路径: {}", outputFilePath);
    }

    /**
     * 向队列添加数据
     */
    public void enqueue(String data) {
        if (!isRunning.get()) {
            LOG.error("异步写入线程未启动，无法添加数据");
            return;
        }

        try {
            dataQueue.put(data); // 队列满时会阻塞
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("添加数据到队列被中断", e);
        }
    }

    /**
     * 停止异步写入线程（确保所有数据写入完成）
     */
    public void stop() {
        if (!isRunning.get()) {
            return;
        }

        isRunning.set(false);
        // 中断写入线程
        if (writerThread != null) {
            writerThread.interrupt();
            try {
                writerThread.join(5000); // 等待最多5秒
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.error("等待写入线程结束被中断", e);
            }
        }

        // 关闭文件
        if (printWriter != null) {
            printWriter.flush();
            printWriter.close();
        }
        LOG.info("异步文件写入线程已停止，共写入 {} 条数据", dataQueue.size() - dataQueue.remainingCapacity());
    }

    /**
     * 循环从队列取数据并写入文件
     */
    private void writeLoop() {
        while (isRunning.get() || !dataQueue.isEmpty()) {
            try {
                String data = dataQueue.poll(); // 非阻塞获取
                if (data != null) {
                    printWriter.println(data);
                    // 每100条刷新一次，平衡性能和实时性
                    if (dataQueue.size() % 100 == 0) {
                        printWriter.flush();
                    }
                } else {
                    // 队列空时短暂休眠，减少CPU占用
                    Thread.sleep(10);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("写入线程被中断");
                break;
            } catch (Exception e) {
                LOG.error("写入文件失败", e);
            }
        }
        // 最后强制刷新
        if (printWriter != null) {
            printWriter.flush();
        }
    }
}
