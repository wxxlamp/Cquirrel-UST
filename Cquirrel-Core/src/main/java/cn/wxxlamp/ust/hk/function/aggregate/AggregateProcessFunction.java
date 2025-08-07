package cn.wxxlamp.ust.hk.function.aggregate;

import cn.wxxlamp.ust.hk.entity.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.wxxlamp.ust.hk.exception.DataProcessException;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;

/**
 * Process function for aggregating line item data to calculate revenue.
 * This function maintains state for each key and updates the aggregated result
 * based on incoming line items with AGGREGATE or AGGREGATE_DELETE operations.
 *
 * @author wxx
 * @version 2025-08-03 17:18
 */
public class AggregateProcessFunction extends KeyedProcessFunction<String, LineItem, Result> {

    private static final Logger LOG = LoggerFactory.getLogger(AggregateProcessFunction.class);

    private static final String FUNCTION_NAME = "AggregateProcessFunction";

    /** State to store the aggregated result for each key */
    private ValueState<Result> resultState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // Step 1: Initialize the state for storing aggregated results
        resultState = getRuntimeContext().getState(
                new ValueStateDescriptor<>(FUNCTION_NAME + "_result",
                        TypeInformation.of(Result.class)));
    }

    @Override
    public void processElement(LineItem lineItem, Context ctx, Collector<Result> out) {
        try {
            LOG.debug("Processing aggregation data: {}", lineItem.getKeyValue());

            // Step 1: Initialize result state if it doesn't exist
            if (resultState.value() == null) {
                resultState.update(new Result(lineItem));
            }

            // Step 2: Get current result and calculate revenue for the line item
            Result result = resultState.value();
            BigDecimal revenue = lineItem.calculateRevenue();

            // Step 3: Update the aggregated result based on operation type
            switch (lineItem.getOperationType()) {
                case AGGREGATE:
                    result.addRevenue(revenue);
                    break;
                case AGGREGATE_DELETE:
                    result.subtractRevenue(revenue);
                    break;
                default:
                    LOG.warn("Unsupported aggregation operation type: {}", lineItem.getOperationType());
                    return;
            }

            // Step 4: Update state and emit result
            resultState.update(result);
            LOG.info("Aggregation result updated: {}", result);
            out.collect(result);
        } catch (Exception e) {
            LOG.error("Aggregation calculation error", e);
            throw new DataProcessException("Aggregation calculation error", e);
        }
    }
}