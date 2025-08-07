package cn.wxxlamp.ust.hk.function.groupby;

import cn.wxxlamp.ust.hk.entity.Customer;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * Key selector for grouping Customer entities by their key value.
 * This class implements Flink's KeySelector interface to extract the grouping key
 * from Customer objects, which is used in keyBy operations.
 *
 * @author wxx
 * @version 2025-08-03 22:55
 */
public class CustomerGroupBy implements KeySelector<Customer, String> {

    /**
     * Get the key value for grouping a Customer entity
     * @param value The Customer entity
     * @return The key value used for grouping
     * @throws Exception if there's an error extracting the key
     */
    @Override
    public String getKey(Customer value) throws Exception {
        return value.getKeyValue();
    }
}
