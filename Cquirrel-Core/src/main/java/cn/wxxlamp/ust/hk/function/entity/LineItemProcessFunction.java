package cn.wxxlamp.ust.hk.function.entity;

import cn.wxxlamp.ust.hk.constant.OperationType;
import cn.wxxlamp.ust.hk.constant.TpcHConstants;
import cn.wxxlamp.ust.hk.entity.BaseEntity;
import cn.wxxlamp.ust.hk.entity.LineItem;
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
 * Process function for handling the join between Orders and LineItem entities.
 * This function manages state for active orders and line items, and creates associations
 * between them based on key relationships for aggregation.
 *
 * @author wxx
 * @version 2025-08-03 17:12
 */
public class LineItemProcessFunction extends KeyedCoProcessFunction<String, Orders, LineItem, LineItem> {

    private static final Logger LOG = LoggerFactory.getLogger(LineItemProcessFunction.class);
    private static final String FUNCTION_NAME = "LineItemProcessFunction";

    /** State to store active line items */
    private ValueState<HashSet<LineItem>> aliveLineItemsState;
    
    /** State to count active orders */
    private ValueState<Integer> counterState;
    
    /** State to store the last active order */
    private ValueState<Orders> lastAliveOrderState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // Step 1: Initialize state for active line items
        aliveLineItemsState = getRuntimeContext().getState(
                new ValueStateDescriptor<>(FUNCTION_NAME + "_aliveLineItems",
                        TypeInformation.of(new TypeHint<HashSet<LineItem>>() {
                        })));

        // Step 2: Initialize counter state for active orders
        counterState = getRuntimeContext().getState(
                new ValueStateDescriptor<>(FUNCTION_NAME + "_counter",
                        IntegerTypeInfo.of(new TypeHint<Integer>() {})));

        // Step 3: Initialize state for the last active order
        lastAliveOrderState = getRuntimeContext().getState(
                new ValueStateDescriptor<>(FUNCTION_NAME + "_lastAliveOrder",
                        TypeInformation.of(new TypeHint<Orders>() {})));
    }

    @Override
    public void processElement1(Orders order, Context ctx, Collector<LineItem> out) throws Exception {
        try {
            // Step 1: Initialize state if needed
            initState();
            LOG.debug("Processing orders stream: {}", order.getFieldValue(TpcHConstants.FIELD_O_ORDERKEY));

            // Step 2: Process order based on operation type
            switch (order.getOperationType()) {
                case SET_ALIVE_RIGHT:
                    // Step 2a: Update the last alive order and increment counter
                    lastAliveOrderState.update(order);
                    counterState.update(counterState.value() + 1);

                    // Step 2b: Associate with all active line items
                    for (BaseEntity lineItem : aliveLineItemsState.value()) {
                        LineItem newLineItem = new LineItem();
                        newLineItem.mergeFields(lineItem);
                        newLineItem.mergeFields(order);
                        newLineItem.setOperationType(OperationType.AGGREGATE);
                        newLineItem.setKeyValue(generateGroupKey(newLineItem));
                        out.collect(newLineItem);
                    }
                    break;

                case SET_DEAD_RIGHT:
                    // Step 2c: Clear last alive order and decrement counter
                    lastAliveOrderState.update(null);
                    counterState.update(counterState.value() - 1);

                    // Step 2d: Disassociate from all active line items
                    for (BaseEntity lineItem : aliveLineItemsState.value()) {
                        LineItem newLineItem = new LineItem();
                        newLineItem.mergeFields(lineItem);
                        newLineItem.mergeFields(order);
                        newLineItem.setOperationType(OperationType.AGGREGATE_DELETE);
                        newLineItem.setKeyValue(generateGroupKey(newLineItem));
                        out.collect(newLineItem);
                    }
                    break;

                default:
                    LOG.warn("Unsupported order operation type: {}", order.getOperationType());
            }
        } catch (Exception e) {
            LOG.error("Error processing orders stream", e);
            throw new DataProcessException("Error processing orders stream", e);
        }
    }

    @Override
    public void processElement2(LineItem item, Context ctx, Collector<LineItem> out) throws Exception {
        try {
            // Step 1: Initialize state if needed
            initState();
            LOG.debug("Processing line item stream: {}", item.getFieldValue(TpcHConstants.FIELD_L_ORDERKEY));

            // Step 2: Filter line items by ship date threshold
            if (!item.isShipDateAfterThreshold()) {
                LOG.debug("Filtering line item with ship date before threshold: {}", item.getFieldValue(TpcHConstants.FIELD_L_ORDERKEY));
                return;
            }

            // Step 3: Process line items based on operation type
            switch (item.getOperationType()) {
                case INSERT:
                    // Step 3a: Add line item to active line items set
                    aliveLineItemsState.value().add(item);
                    aliveLineItemsState.update(aliveLineItemsState.value());

                    // Step 3b: If there's an active order, establish association
                    if (counterState.value() > 0 && lastAliveOrderState.value() != null) {
                        LineItem newLineItem = new LineItem();
                        newLineItem.mergeFields(item);
                        newLineItem.mergeFields(lastAliveOrderState.value());
                        newLineItem.setOperationType(OperationType.AGGREGATE);
                        newLineItem.setKeyValue(generateGroupKey(newLineItem));
                        out.collect(newLineItem);
                    }
                    break;

                case DELETE:
                    // Step 3c: If there's an active order, disassociate
                    if (counterState.value() > 0 && lastAliveOrderState.value() != null) {
                        LineItem newLineItem = new LineItem();
                        newLineItem.mergeFields(item);
                        newLineItem.mergeFields(lastAliveOrderState.value());
                        newLineItem.setOperationType(OperationType.AGGREGATE_DELETE);
                        newLineItem.setKeyValue(generateGroupKey(newLineItem));
                        out.collect(newLineItem);
                    }

                    // Step 3d: Remove line item from active line items set
                    aliveLineItemsState.value().remove(item);
                    aliveLineItemsState.update(aliveLineItemsState.value());
                    break;

                default:
                    LOG.warn("Unsupported line item operation type: {}", item.getOperationType());
            }
        } catch (Exception e) {
            LOG.error("Error processing line item stream", e);
            throw new DataProcessException("Error processing line item stream", e);
        }
    }

    /**
     * Generate a grouping key (l_orderkey, o_orderdate, o_shippriority)
     * @param lineItem The line item to generate a key for
     * @return The generated grouping key
     */
    private String generateGroupKey(LineItem lineItem) {
        return lineItem.getFieldValue(TpcHConstants.FIELD_L_ORDERKEY) + "|" +
                lineItem.getFieldValue(TpcHConstants.FIELD_O_ORDERDATE) + "|" +
                lineItem.getFieldValue(TpcHConstants.FIELD_O_SHIPPRIORITY);
    }

    /**
     * Initialize state values if they are null
     * @throws IOException if there's an error initializing state
     */
    private void initState() throws IOException {
        if (counterState.value() == null) {
            counterState.update(0);
        }
        if (aliveLineItemsState.value() == null) {
            aliveLineItemsState.update(new HashSet<>());
        }
    }
}