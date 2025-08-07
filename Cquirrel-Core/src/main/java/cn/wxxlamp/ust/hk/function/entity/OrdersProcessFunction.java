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
 * Process function for handling the join between Customer and Orders entities.
 * This function manages state for active customers and orders, and creates associations
 * between them based on key relationships.
 *
 * @author wxx
 * @version 2025-08-03 17:15
 */
public class OrdersProcessFunction extends KeyedCoProcessFunction<String, Customer, Orders, Orders> {

    private static final Logger LOG = LoggerFactory.getLogger(OrdersProcessFunction.class);
    private static final String FUNCTION_NAME = "OrdersProcessFunction";

    /** State to store active orders */
    private ValueState<HashSet<Orders>> aliveOrdersState;
    
    /** State to count active customers */
    private ValueState<Integer> counterState;
    
    /** State to store the last active customer */
    private ValueState<Customer> lastAliveCustomerState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // Step 1: Initialize state for active orders
        aliveOrdersState = getRuntimeContext().getState(
                new ValueStateDescriptor<>(FUNCTION_NAME + "_aliveOrders",
                        TypeInformation.of(new TypeHint<HashSet<Orders>>() {
                        })));

        // Step 2: Initialize counter state for active customers
        counterState = getRuntimeContext().getState(
                new ValueStateDescriptor<>(FUNCTION_NAME + "_counter",
                        IntegerTypeInfo.INT_TYPE_INFO));

        // Step 3: Initialize state for the last active customer
        lastAliveCustomerState = getRuntimeContext().getState(
                new ValueStateDescriptor<>(FUNCTION_NAME + "_lastAliveCustomer",
                        TypeInformation.of(new TypeHint<Customer>() {
                        })));
    }

    @Override
    public void processElement1(Customer customer, Context ctx, Collector<Orders> out) {
        try {
            // Step 1: Initialize state if needed
            initState();
            LOG.debug("Processing customer stream: {}", customer.getFieldValue(TpcHConstants.FIELD_C_CUSTKEY));

            // Step 2: Process customer based on operation type
            switch (customer.getOperationType()) {
                case SET_ALIVE:
                    // Step 2a: Update the last alive customer and increment counter
                    lastAliveCustomerState.update(customer);
                    counterState.update(counterState.value() + 1);

                    // Step 2b: Associate with all active orders
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
                    // Step 2c: Clear last alive customer and decrement counter
                    lastAliveCustomerState.update(null);
                    counterState.update(counterState.value() - 1);

                    // Step 2d: Disassociate from all active orders
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
                    LOG.warn("Unsupported customer operation type: {}", customer.getOperationType());
            }
        } catch (Exception e) {
            LOG.error("Error processing customer stream", e);
            throw new DataProcessException("Error processing customer stream", e);
        }
    }

    @Override
    public void processElement2(Orders orders, Context ctx, Collector<Orders> out) throws Exception {
        try {
            // Step 1: Initialize state if needed
            initState();
            LOG.debug("Processing orders stream: {}", orders.getFieldValue(TpcHConstants.FIELD_O_ORDERKEY));

            // Step 2: Filter orders by date threshold
            if (!orders.isOrderDateBeforeThreshold()) {
                LOG.debug("Filtering order with date after threshold: {}", orders.getFieldValue(TpcHConstants.FIELD_O_ORDERKEY));
                return;
            }

            // Step 3: Process orders based on operation type
            switch (orders.getOperationType()) {
                case INSERT:
                    // Step 3a: Add order to active orders set
                    aliveOrdersState.value().add(orders);
                    aliveOrdersState.update(aliveOrdersState.value());

                    // Step 3b: If there's an active customer, establish association
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
                    // Step 3c: If there's an active customer, disassociate
                    if (counterState.value() > 0 && lastAliveCustomerState.value() != null) {
                        Orders newOrder = new Orders();
                        newOrder.mergeFields(orders);
                        newOrder.mergeFields(lastAliveCustomerState.value());
                        newOrder.setOperationType(OperationType.SET_DEAD_RIGHT);
                        newOrder.setKeyValue(newOrder.getFieldValue(TpcHConstants.FIELD_O_ORDERKEY));
                        out.collect(newOrder);
                    }

                    // Step 3d: Remove order from active orders set
                    aliveOrdersState.value().remove(orders);
                    aliveOrdersState.update(aliveOrdersState.value());
                    break;

                default:
                    LOG.warn("Unsupported order operation type: {}", orders.getOperationType());
            }
        } catch (Exception e) {
            LOG.error("Error processing orders stream", e);
            throw new DataProcessException("Error processing orders stream", e);
        }
    }

    /**
     * Initialize state values if they are null
     * @throws IOException if there's an error initializing state
     */
    private void initState() throws IOException {
        if (counterState.value() == null) {
            counterState.update(0);
        }
        if (aliveOrdersState.value() == null) {
            aliveOrdersState.update(new HashSet<>());
        }
    }
}
