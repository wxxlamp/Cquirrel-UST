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
 * @author wxx
 * @version 2025-08-03 17:09
 */
public class CustomerProcessFunction extends KeyedProcessFunction<String, Customer, Customer> {

    private static final Logger LOG = LoggerFactory.getLogger(CustomerProcessFunction.class);

    @Override
    public void processElement(Customer customer, Context ctx, Collector<Customer> out) throws Exception {
        try {

            // 过滤汽车市场客户
            if (customer.isAutomobileSegment()) {
                switch (customer.getOperationType()) {
                    case INSERT:
                        customer.setOperationType(OperationType.SET_ALIVE);
                        customer.setKeyValue(customer.getFieldValue(TpcHConstants.FIELD_C_CUSTKEY));
                        out.collect(customer);
                        LOG.debug("处理客户INSERT操作: {}", customer.getFieldValue(TpcHConstants.FIELD_C_CUSTKEY));
                        break;
                    case DELETE:
                        customer.setOperationType(OperationType.SET_DEAD);
                        customer.setKeyValue(customer.getFieldValue(TpcHConstants.FIELD_C_CUSTKEY));
                        out.collect(customer);
                        LOG.debug("处理客户DELETE操作: {}", customer.getFieldValue(TpcHConstants.FIELD_C_CUSTKEY));
                        break;
                    default:
                        LOG.warn("不支持的客户操作类型: {}", customer.getOperationType());
                }
            } else {
                LOG.debug("过滤非汽车市场客户: {}", customer.getFieldValue(TpcHConstants.FIELD_C_CUSTKEY));
            }
        } catch (Exception e) {
            LOG.error("处理客户数据异常", e);
            throw new DataProcessException("处理客户数据异常", e);
        }
    }
}
