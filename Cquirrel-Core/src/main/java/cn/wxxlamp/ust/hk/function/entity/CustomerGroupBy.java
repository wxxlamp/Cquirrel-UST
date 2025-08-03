package cn.wxxlamp.ust.hk.function.entity;

import cn.wxxlamp.ust.hk.entity.Customer;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * @author wxx
 * @version 2025-08-03 22:55
 */
public class CustomerGroupBy implements KeySelector<Customer, String> {

    @Override
    public String getKey(Customer value) throws Exception {
        return value.getKeyValue();
    }
}
