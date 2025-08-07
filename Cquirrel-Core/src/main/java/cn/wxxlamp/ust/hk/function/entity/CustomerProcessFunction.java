package cn.wxxlamp.ust.hk.function.entity;

import cn.wxxlamp.ust.hk.constant.OperationType;
import cn.wxxlamp.ust.hk.constant.TpcHConstants;
import cn.wxxlamp.ust.hk.entity.Customer;
import cn.wxxlamp.ust.hk.exception.DataProcessException;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Process function for handling Customer entities in the TPC-H Q3 processing pipeline.
 * This function filters customers based on market segment and transforms operation types.
 *
 * @author wxx
 * @version 2025-08-03 17:09
 */
public class CustomerProcessFunction extends KeyedProcessFunction<String, Customer, Customer> {

    private static final Logger LOG = LoggerFactory.getLogger(CustomerProcessFunction.class);

    @Override
    public void processElement(Customer customer, Context ctx, Collector<Customer> out) {
        try {
            // Step 1: Filter for automobile market segment customers only
            if (customer.isAutomobileSegment()) {
                // Step 2: Process based on operation type
                switch (customer.getOperationType()) {
                    case INSERT:
                        // Transform INSERT operation to SET_ALIVE
                        customer.setOperationType(OperationType.SET_ALIVE);
                        customer.setKeyValue(customer.getFieldValue(TpcHConstants.FIELD_C_CUSTKEY));
                        out.collect(customer);
                        LOG.debug("Processing customer INSERT operation: {}", customer.getFieldValue(TpcHConstants.FIELD_C_CUSTKEY));
                        break;
                    case DELETE:
                        // Transform DELETE operation to SET_DEAD
                        customer.setOperationType(OperationType.SET_DEAD);
                        customer.setKeyValue(customer.getFieldValue(TpcHConstants.FIELD_C_CUSTKEY));
                        out.collect(customer);
                        LOG.debug("Processing customer DELETE operation: {}", customer.getFieldValue(TpcHConstants.FIELD_C_CUSTKEY));
                        break;
                    default:
                        LOG.warn("Unsupported customer operation type: {}", customer.getOperationType());
                }
            } else {
                LOG.debug("Filtering non-automobile market customer: {}", customer.getFieldValue(TpcHConstants.FIELD_C_CUSTKEY));
            }
        } catch (Exception e) {
            LOG.error("Error processing customer data", e);
            throw new DataProcessException("Error processing customer data", e);
        }
    }
}
