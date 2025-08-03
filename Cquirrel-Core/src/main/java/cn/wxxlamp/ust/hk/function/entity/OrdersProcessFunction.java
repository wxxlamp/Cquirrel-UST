package cn.wxxlamp.ust.hk.function.entity;

import cn.wxxlamp.ust.hk.constant.OperationType;
import cn.wxxlamp.ust.hk.constant.TpcHConstants;
import cn.wxxlamp.ust.hk.entity.Customer;
import cn.wxxlamp.ust.hk.entity.Orders;
import cn.wxxlamp.ust.hk.exception.DataProcessException;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.IntegerTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;

/**
 * @author wxx
 * @version 2025-08-03 17:15
 */
public class OrdersProcessFunction extends KeyedCoProcessFunction<String, Customer, Orders, Orders> {
    private static final Logger LOG = LoggerFactory.getLogger(OrdersProcessFunction.class);
    private static final String FUNCTION_NAME = "OrdersProcessFunction";

    private ValueState<HashSet<Orders>> aliveOrdersState;
    private ValueState<Integer> counterState;
    private ValueState<Customer> lastAliveCustomerState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // 初始化状态
        aliveOrdersState = getRuntimeContext().getState(
                new ValueStateDescriptor<>(FUNCTION_NAME + "_aliveOrders",
                        TypeInformation.of(new TypeHint<>() {
                        })));

        counterState = getRuntimeContext().getState(
                new ValueStateDescriptor<>(FUNCTION_NAME + "_counter",
                        IntegerTypeInfo.INT_TYPE_INFO));

        lastAliveCustomerState = getRuntimeContext().getState(
                new ValueStateDescriptor<>(FUNCTION_NAME + "_lastAliveCustomer",
                        TypeInformation.of(new TypeHint<>() {
                        })));
    }

    @Override
    public void processElement1(Customer customer, Context ctx, Collector<Orders> out) {
        try {
            initState();
            LOG.debug("处理客户数据流: {}", customer.getFieldValue(TpcHConstants.FIELD_C_CUSTKEY));

            switch (customer.getOperationType()) {
                case SET_ALIVE:
                    lastAliveCustomerState.update(customer);
                    counterState.update(counterState.value() + 1);

                    // 与所有存活的订单关联
                    for (Orders order : aliveOrdersState.value()) {
                        Orders newOrder = new Orders();
                        newOrder.mergeFields(order);
                        newOrder.mergeFields(customer);
                        newOrder.setOperationType(OperationType.SET_ALIVE_RIGHT);
                        newOrder.setKeyValue(newOrder.getFieldValue(TpcHConstants.FIELD_O_ORDERKEY));
                        out.collect(newOrder);
                    }
                    break;

                case SET_DEAD:
                    lastAliveCustomerState.update(null);
                    counterState.update(counterState.value() - 1);

                    // 与所有存活的订单解除关联
                    for (Orders order : aliveOrdersState.value()) {
                        Orders newOrder = new Orders();
                        newOrder.mergeFields(order);
                        newOrder.mergeFields(customer);
                        newOrder.setOperationType(OperationType.SET_DEAD_RIGHT);
                        newOrder.setKeyValue(newOrder.getFieldValue(TpcHConstants.FIELD_O_ORDERKEY));
                        out.collect(newOrder);
                    }
                    break;

                default:
                    LOG.warn("不支持的客户操作类型: {}", customer.getOperationType());
            }
        } catch (Exception e) {
            LOG.error("处理客户数据流异常", e);
            throw new DataProcessException("处理客户数据流异常", e);
        }
    }

    @Override
    public void processElement2(Orders orders, Context ctx, Collector<Orders> out) throws Exception {
        try {
            initState();
            LOG.debug("处理订单数据流: {}", orders.getFieldValue(TpcHConstants.FIELD_O_ORDERKEY));

            // 过滤订单日期
            if (!orders.isOrderDateBeforeThreshold()) {
                LOG.debug("过滤订单日期不符合条件的订单: {}", orders.getFieldValue(TpcHConstants.FIELD_O_ORDERKEY));
                return;
            }

            switch (orders.getOperationType()) {
                case INSERT:
                    aliveOrdersState.value().add(orders);
                    aliveOrdersState.update(aliveOrdersState.value());

                    // 如果有存活的客户，建立关联
                    if (counterState.value() > 0 && lastAliveCustomerState.value() != null) {
                        Orders newOrder = new Orders();
                        newOrder.mergeFields(orders);
                        newOrder.mergeFields(lastAliveCustomerState.value());
                        newOrder.setOperationType(OperationType.SET_ALIVE_RIGHT);
                        newOrder.setKeyValue(newOrder.getFieldValue(TpcHConstants.FIELD_O_ORDERKEY));
                        out.collect(newOrder);
                    }
                    break;

                case DELETE:
                    // 如果有存活的客户，解除关联
                    if (counterState.value() > 0 && lastAliveCustomerState.value() != null) {
                        Orders newOrder = new Orders();
                        newOrder.mergeFields(orders);
                        newOrder.mergeFields(lastAliveCustomerState.value());
                        newOrder.setOperationType(OperationType.SET_DEAD_RIGHT);
                        newOrder.setKeyValue(newOrder.getFieldValue(TpcHConstants.FIELD_O_ORDERKEY));
                        out.collect(newOrder);
                    }

                    aliveOrdersState.value().remove(orders);
                    aliveOrdersState.update(aliveOrdersState.value());
                    break;

                default:
                    LOG.warn("不支持的订单操作类型: {}", orders.getOperationType());
            }
        } catch (Exception e) {
            LOG.error("处理订单数据流异常", e);
            throw new DataProcessException("处理订单数据流异常", e);
        }
    }


    private void initState() throws IOException {
        if (counterState.value() == null) {
            counterState.update(0);
        }
        if (aliveOrdersState.value() == null) {
            aliveOrdersState.update(new HashSet<>());
        }
    }
}
