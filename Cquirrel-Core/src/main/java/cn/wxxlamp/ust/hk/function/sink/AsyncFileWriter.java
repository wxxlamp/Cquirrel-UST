package cn.wxxlamp.ust.hk.function.sink;

import cn.wxxlamp.ust.hk.constant.FilePathConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Asynchronously writes results to a file using a background thread.
 * This class handles the file writing in a non-blocking manner by using a queue
 * to buffer data before writing to disk.
 *
 * @author wxx
 * @version 2025-08-06 22:22
 */
public class AsyncFileWriter implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(AsyncFileWriter.class);
    private final BlockingQueue<String> dataQueue;
    private final String outputFilePath;
    private final AtomicBoolean isRunning;
    private Thread writerThread;
    private PrintWriter printWriter;

    // Header for the output file
    private static final String HEADER = "l_orderkey, o_orderdate, o_shippriority, revenue";

    public AsyncFileWriter() {
        this.dataQueue = new LinkedBlockingQueue<>(10000); // Queue capacity limit
        this.outputFilePath = FilePathConstants.OUTPUT_FILE_PATH;
        this.isRunning = new AtomicBoolean(false);
    }

    public static AsyncFileWriter create() {
        return new AsyncFileWriter();
    }

    /**
     * Starts the asynchronous writing thread.
     *
     * @throws IOException if there's an error initializing the file writer
     */
    public synchronized void start() throws IOException {
        if (isRunning.get()) {
            LOG.warn("Async writer thread already started");
            return;
        }

        // Initialize the file writer (overwrite mode)
        printWriter = new PrintWriter(new FileWriter(outputFilePath, false));
        // Write header
        printWriter.println(HEADER);
        printWriter.flush();

        isRunning.set(true);
        writerThread = new Thread(this::writeLoop, "Async-File-Writer");
        writerThread.start();
        LOG.info("Async file writer thread started, output path: {}", outputFilePath);
    }

    /**
     * Enqueues data to be written to the file.
     *
     * @param data The data string to be written
     */
    public void enqueue(String data) {
        if (!isRunning.get()) {
            LOG.error("Async writer thread not started, cannot add data");
            return;
        }

        try {
            dataQueue.put(data); // Will block if queue is full
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Adding data to queue was interrupted", e);
        }
    }

    /**
     * Stops the asynchronous writing thread (ensuring all data is written).
     */
    public void stop() {
        if (!isRunning.get()) {
            return;
        }

        isRunning.set(false);
        // Interrupt the writer thread
        if (writerThread != null) {
            writerThread.interrupt();
            try {
                writerThread.join(5000); // Wait up to 5 seconds
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.error("Waiting for writer thread to finish was interrupted", e);
            }
        }

        // Close the file
        if (printWriter != null) {
            printWriter.flush();
            printWriter.close();
        }
        LOG.info("Async file writer thread stopped, {} data records written",
                dataQueue.size() - dataQueue.remainingCapacity());
    }

    /**
     * Continuously retrieves data from the queue and writes it to the file.
     */
    private void writeLoop() {
        while (isRunning.get() || !dataQueue.isEmpty()) {
            try {
                String data = dataQueue.poll(); // Non-blocking retrieval
                if (data != null) {
                    printWriter.println(data);
                    // Flush every 100 records to balance performance and real-time behavior
                    if (dataQueue.size() % 100 == 0) {
                        printWriter.flush();
                    }
                } else {
                    // Briefly sleep when queue is empty to reduce CPU usage
                    Thread.sleep(10);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("Writer thread was interrupted");
                break;
            } catch (Exception e) {
                LOG.error("Failed to write to file", e);
            }
        }
        // Final flush
        if (printWriter != null) {
            printWriter.flush();
        }
    }
}
