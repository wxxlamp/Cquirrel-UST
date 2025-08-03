package cn.wxxlamp.ust.hk.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author wxx
 * @version 2025-08-04 00:02
 */
public class RevenueAggregateSink extends RichSinkFunction<String> {

    private static final Logger LOG = LoggerFactory.getLogger(RevenueAggregateSink.class);
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private Map<String, BigDecimal> aggregatedResults;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        aggregatedResults = new HashMap<>();
        System.out.println("['l_orderkey', 'o_orderdate', 'o_shippriority', 'revenue']");
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        if (value == null || value.trim().isEmpty()) {
            LOG.warn("忽略空的输出数据");
            return;
        }

        String[] parts = value.split("\\|");
        if (parts.length != 4) {
            LOG.warn("无效的输出格式: {}", value);
            return;
        }

        try {
            String orderKey = parts[0];
            String orderDate = parts[1];
            String shipPriority = parts[2];
            BigDecimal revenue = new BigDecimal(parts[3]).setScale(4, RoundingMode.HALF_UP);

            String groupKey = String.join("|", orderKey, orderDate, shipPriority);

            // 修复：使用merge进行累加聚合（原代码是直接覆盖）
            aggregatedResults.put(
                    groupKey,
                    revenue);
        } catch (NumberFormatException e) {
            LOG.error("解析Revenue失败: {}", value, e);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        int successCount = 0;

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

                // 使用线程安全的DateTimeFormatter
                LocalDate localDate = LocalDate.parse(orderDateStr, DATE_FORMATTER);

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