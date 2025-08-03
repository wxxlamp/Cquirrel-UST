package cn.wxxlamp.ust.hk.function.entity;

import cn.wxxlamp.ust.hk.entity.Orders;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * @author wxx
 * @version 2025-08-03 22:57
 */
public class OrdersGroupBy implements KeySelector<Orders, String> {
    @Override
    public String getKey(Orders value) {
        return value.getKeyValue();
    }
}