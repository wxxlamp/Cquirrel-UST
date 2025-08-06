package cn.wxxlamp.ust.hk.function.sink;

import cn.wxxlamp.ust.hk.entity.Result;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author wxx
 * @version 2025-08-06
 */
public class RevenueAggregateSink extends RichSinkFunction<Result> {

    private static final Logger LOG = LoggerFactory.getLogger(RevenueAggregateSink.class);
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private Map<String, BigDecimal> aggregatedResults;
    private PrintWriter fileWriter;
    private String outputFilePath;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        aggregatedResults = new HashMap<>();
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

        LOG.info("task finish, total size is {}", aggregatedResults.size());
        for (Map.Entry<String, BigDecimal> entry : aggregatedResults.entrySet()) {
            String groupKey = entry.getKey();
            BigDecimal totalRevenue = entry.getValue();

            if (totalRevenue.compareTo(BigDecimal.ZERO) < 0) {
                continue;
            }

            try {
                String[] keys = groupKey.split("\\|", 3); // 限制分割为3部分
                if (keys.length < 3) {
                    LOG.warn("分组键格式错误: {}", groupKey);
                    continue;
                }

                String orderKey = keys[0];
                String orderDateStr = keys[1];
                String shipPriority = keys[2];

                LocalDate localDate = LocalDate.parse(orderDateStr, DATE_FORMATTER);

                System.out.println(orderDateStr);
                String formattedOutput = String.format(
                        "(%s, datetime.date(%d, %d, %d), %s, Decimal('%s'))",
                        orderKey,
                        localDate.getYear(),
                        localDate.getMonthValue(),
                        localDate.getDayOfMonth(),
                        shipPriority,
                        totalRevenue.setScale(4, RoundingMode.HALF_UP).toPlainString()
                );
                System.out.println(formattedOutput);
                successCount++;
            } catch (DateTimeParseException e) {
                LOG.error("日期解析失败: {}", groupKey, e);
            } catch (Exception e) {
                LOG.error("处理记录失败 (key: {}): {}", groupKey, e.getMessage(), e);
            }
        }
        LOG.info("聚合结果输出完成，共{}条有效记录", successCount);
    }
}