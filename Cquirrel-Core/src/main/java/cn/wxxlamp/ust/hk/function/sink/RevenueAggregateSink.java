package cn.wxxlamp.ust.hk.function.sink;

import cn.wxxlamp.ust.hk.entity.Result;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * Sink function for outputting the final aggregated revenue results.
 * This function collects results, aggregates them, and outputs them in a formatted way.
 *
 * @author wxx
 * @version 2025-08-06
 */
public class RevenueAggregateSink extends RichSinkFunction<Result> {

    private static final Logger LOG = LoggerFactory.getLogger(RevenueAggregateSink.class);
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    /** Map to store aggregated results by group key */
    private Map<String, BigDecimal> aggregatedResults;

    /** Output file path (not currently used) */
    private String outputFilePath;

    private AsyncFileWriter asyncFileWriter;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // Step 1: Initialize the map for storing aggregated results
        aggregatedResults = new HashMap<>();
        asyncFileWriter = AsyncFileWriterHandler.getAsyncFileWriter();
    }

    @Override
    public void invoke(Result result, Context context) {
        // Step 1: Validate input
        if (result == null) {
            LOG.warn("Ignoring null output data");
            return;
        }

        try {
            // Step 2: Extract result fields
            String orderKey = result.getOrderKey();
            String orderDate = result.getOrderDate();
            String shipPriority = result.getShipPriority();
            BigDecimal revenue = BigDecimal.valueOf(result.getRevenue())
                    .setScale(4, RoundingMode.HALF_UP);

            // Step 3: Create group key and store result
            String groupKey = String.join("|", orderKey, orderDate, shipPriority);

            // Step 4: Accumulate aggregation results
            aggregatedResults.put(groupKey, revenue);
        } catch (NumberFormatException e) {
            LOG.error("Failed to parse Revenue", e);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        int successCount = 0;

        LOG.info("Task finished, total size is {}", aggregatedResults.size());

        asyncFileWriter.start();

        // Step 1: Process each aggregated result
        LOG.info("task finish, total size is {}", aggregatedResults.size());
        for (Map.Entry<String, BigDecimal> entry : aggregatedResults.entrySet()) {
            String groupKey = entry.getKey();
            BigDecimal totalRevenue = entry.getValue();

            // Step 2: Skip negative revenue values
            if (totalRevenue.compareTo(BigDecimal.ZERO) < 0) {
                continue;
            }

            try {
                // Step 3: Parse group key components
                String[] keys = groupKey.split("\\|", 3); // Limit to 3 parts
                if (keys.length < 3) {
                    LOG.warn("Invalid group key format: {}", groupKey);
                    continue;
                }

                String orderKey = keys[0];
                String orderDateStr = keys[1];
                String shipPriority = keys[2];

                // Step 5: Format and output result
                System.out.println(orderDateStr);
                String formattedOutput = String.format(
                        "%s, %s, %s, %s",
                        orderKey,
                        orderDateStr,
                        shipPriority,
                        totalRevenue.setScale(4, RoundingMode.HALF_UP).toPlainString()
                );
                // print in async queue
                asyncFileWriter.enqueue(formattedOutput);
                successCount++;
            } catch (Exception e) {
                LOG.error("Failed to process record (key: {}): {}", groupKey, e.getMessage(), e);
            }
        }
        LOG.info("Aggregation results output completed, {} valid records in total", successCount);
    }
}