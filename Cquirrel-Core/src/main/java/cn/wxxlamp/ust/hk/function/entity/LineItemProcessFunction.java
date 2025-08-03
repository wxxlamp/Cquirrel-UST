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
 * @author wxx
 * @version 2025-08-03 17:12
 */
public class LineItemProcessFunction extends KeyedCoProcessFunction<String, Orders, LineItem, LineItem> {

    private static final Logger LOG = LoggerFactory.getLogger(LineItemProcessFunction.class);
    private static final String FUNCTION_NAME = "LineItemProcessFunction";

    private ValueState<HashSet<LineItem>> aliveLineItemsState;
    private ValueState<Integer> counterState;
    private ValueState<Orders> lastAliveOrderState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // 初始化状态
        aliveLineItemsState = getRuntimeContext().getState(
                new ValueStateDescriptor<>(FUNCTION_NAME + "_aliveLineItems",
                        TypeInformation.of(new TypeHint<>() {
                        })));

        counterState = getRuntimeContext().getState(
                new ValueStateDescriptor<>(FUNCTION_NAME + "_counter",
                        IntegerTypeInfo.of(new TypeHint<>() {})));

        lastAliveOrderState = getRuntimeContext().getState(
                new ValueStateDescriptor<>(FUNCTION_NAME + "_lastAliveOrder",
                        IntegerTypeInfo.of(new TypeHint<>() {})));

        initState();
    }

    private void initState() throws IOException {
        if (aliveLineItemsState.value() == null) {
            aliveLineItemsState.update(new HashSet<>());
        }
        if (counterState.value() == null) {
            counterState.update(0);
        }
    }

    @Override
    public void processElement1(Orders order, Context ctx, Collector<LineItem> out) throws Exception {
        try {
            initState();
            LOG.debug("处理订单数据流: {}", order.getFieldValue(TpcHConstants.FIELD_O_ORDERKEY));

            switch (order.getOperationType()) {
                case SET_ALIVE_RIGHT:
                    lastAliveOrderState.update(order);
                    counterState.update(counterState.value() + 1);

                    // 与所有存活的订单项关联
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
                    lastAliveOrderState.update(null);
                    counterState.update(counterState.value() - 1);

                    // 与所有存活的订单项解除关联
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
                    LOG.warn("不支持的订单操作类型: {}", order.getOperationType());
            }
        } catch (Exception e) {
            LOG.error("处理订单数据流异常", e);
            throw new DataProcessException("处理订单数据流异常", e);
        }
    }

    @Override
    public void processElement2(LineItem item, Context ctx, Collector<LineItem> out) throws Exception {
        try {
            initState();
            LOG.debug("处理订单项数据流: {}", item.getFieldValue(TpcHConstants.FIELD_L_ORDERKEY));

            // 过滤发货日期
            if (!item.isShipDateAfterThreshold()) {
                LOG.debug("过滤发货日期不符合条件的订单项: {}", item.getFieldValue(TpcHConstants.FIELD_L_ORDERKEY));
                return;
            }

            switch (item.getOperationType()) {
                case INSERT:
                    aliveLineItemsState.value().add(item);
                    aliveLineItemsState.update(aliveLineItemsState.value());

                    // 如果有存活的订单，建立关联
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
                    // 如果有存活的订单，解除关联
                    if (counterState.value() > 0 && lastAliveOrderState.value() != null) {
                        LineItem newLineItem = new LineItem();
                        newLineItem.mergeFields(item);
                        newLineItem.mergeFields(lastAliveOrderState.value());
                        newLineItem.setOperationType(OperationType.AGGREGATE_DELETE);
                        newLineItem.setKeyValue(generateGroupKey(newLineItem));
                        out.collect(newLineItem);
                    }

                    aliveLineItemsState.value().remove(item);
                    aliveLineItemsState.update(aliveLineItemsState.value());
                    break;

                default:
                    LOG.warn("不支持的订单项操作类型: {}", item.getOperationType());
            }
        } catch (Exception e) {
            LOG.error("处理订单项数据流异常", e);
            throw new DataProcessException("处理订单项数据流异常", e);
        }
    }

    /**
     * 生成分组键 (l_orderkey, o_orderdate, o_shippriority)
     */
    private String generateGroupKey(LineItem lineItem) {
        return lineItem.getFieldValue(TpcHConstants.FIELD_L_ORDERKEY) + "|" +
                lineItem.getFieldValue(TpcHConstants.FIELD_O_ORDERDATE) + "|" +
                lineItem.getFieldValue(TpcHConstants.FIELD_O_SHIPPRIORITY);
    }
}