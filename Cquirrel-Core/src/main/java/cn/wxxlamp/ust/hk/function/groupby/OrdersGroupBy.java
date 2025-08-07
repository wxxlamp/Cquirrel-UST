package cn.wxxlamp.ust.hk.function.groupby;

import cn.wxxlamp.ust.hk.entity.Orders;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * Key selector for grouping Orders entities by their key value.
 * This class implements Flink's KeySelector interface to extract the grouping key
 * from Orders objects, which is used in keyBy operations.
 *
 * @author wxx
 * @version 2025-08-03 22:57
 */
public class OrdersGroupBy implements KeySelector<Orders, String> {
    /**
     * Get the key value for grouping an Orders entity
     * @param value The Orders entity
     * @return The key value used for grouping
     */
    @Override
    public String getKey(Orders value) {
        return value.getKeyValue();
    }
}