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
 * process the final revenue group by keys
 * @author wxx
 * @version 2025-08-06
 */
public class RevenueAggregateSink extends RichSinkFunction<Result> {

    private static final Logger LOG = LoggerFactory.getLogger(RevenueAggregateSink.class);
    private Map<String, BigDecimal> aggregatedResults;

    private AsyncFileWriter asyncFileWriter;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        aggregatedResults = new HashMap<>();
        asyncFileWriter = AsyncFileWriterHandler.getAsyncFileWriter();
    }

    @Override
    public void invoke(Result result, Context context) {
        if (result == null) {
            LOG.warn("忽略空的输出数据");
            return;
        }

        try {
            String orderKey = result.getOrderKey();
            String orderDate = result.getOrderDate();
            String shipPriority = result.getShipPriority();
            BigDecimal revenue = BigDecimal.valueOf(result.getRevenue())
                    .setScale(4, RoundingMode.HALF_UP);

            String groupKey = String.join("|", orderKey, orderDate, shipPriority);

            // 累加聚合结果
            aggregatedResults.put(groupKey, revenue);
        } catch (NumberFormatException e) {
            LOG.error("解析Revenue失败", e);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        int successCount = 0;

        asyncFileWriter.start();

        LOG.info("task finish, total size is {}", aggregatedResults.size());
        for (Map.Entry<String, BigDecimal> entry : aggregatedResults.entrySet()) {
            String groupKey = entry.getKey();
            BigDecimal totalRevenue = entry.getValue();

            if (totalRevenue.compareTo(BigDecimal.ZERO) < 0) {
                continue;
            }

            try {
                String[] keys = groupKey.split("\\|", 3);
                if (keys.length < 3) {
                    LOG.warn("分组键格式错误: {}", groupKey);
                    continue;
                }

                String orderKey = keys[0];
                String orderDateStr = keys[1];
                String shipPriority = keys[2];

                // 格式化输出行（修正原格式错误，去掉多余括号）
                String formattedOutput = String.format(
                        "%s, %s, %s, %s",
                        orderKey,
                        orderDateStr,
                        shipPriority,
                        totalRevenue.setScale(4, RoundingMode.HALF_UP).toPlainString()
                );
                // 放入队列异步写入
                asyncFileWriter.enqueue(formattedOutput);
                successCount++;
            } catch (Exception e) {
                LOG.error("处理记录失败 (key: {}): {}", groupKey, e.getMessage(), e);
            }
        }

        LOG.info("聚合结果输出完成，共{}条有效记录", successCount);
    }
}