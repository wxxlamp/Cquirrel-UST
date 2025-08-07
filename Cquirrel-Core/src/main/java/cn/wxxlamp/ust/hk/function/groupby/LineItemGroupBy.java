package cn.wxxlamp.ust.hk.function.groupby;

import cn.wxxlamp.ust.hk.entity.LineItem;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * Key selector for grouping LineItem entities by their key value.
 * This class implements Flink's KeySelector interface to extract the grouping key
 * from LineItem objects, which is used in keyBy operations.
 *
 * @author wxx
 * @version 2025-08-03 22:57
 */
public class LineItemGroupBy implements KeySelector<LineItem, String> {
    /**
     * Get the key value for grouping a LineItem entity
     * @param value The LineItem entity
     * @return The key value used for grouping
     * @throws Exception if there's an error extracting the key
     */
    @Override
    public String getKey(LineItem value) throws Exception {
        return value.getKeyValue();
    }
}
