package cn.wxxlamp.ust.hk.function;

import cn.wxxlamp.ust.hk.entity.*;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.wxxlamp.ust.hk.exception.DataProcessException;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
/**
 * @author wxx
 * @version 2025-08-03 17:18
 */
public class AggregateProcessFunction extends KeyedProcessFunction<String, LineItem, String> {

    private static final Logger LOG = LoggerFactory.getLogger(AggregateProcessFunction.class);
    private static final String FUNCTION_NAME = "AggregateProcessFunction";

    private ValueState<Result> resultState;

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        super.open(parameters);

        // 初始化状态
        resultState = getRuntimeContext().getState(
                new ValueStateDescriptor<>(FUNCTION_NAME + "_result",
                        TypeInformation.of(Result.class)));
    }

    @Override
    public void processElement(LineItem lineItem, Context ctx, Collector<String> out) throws Exception {
        try {
            LOG.debug("处理聚合数据: {}", lineItem.getKeyValue());

            // 初始化结果状态
            if (resultState.value() == null) {
                resultState.update(new Result(lineItem));
            }

            Result result = resultState.value();
            double revenue = lineItem.calculateRevenue();

            // 根据操作类型更新聚合结果
            switch (lineItem.getOperationType()) {
                case AGGREGATE:
                    result.addRevenue(revenue);
                    break;
                case AGGREGATE_DELETE:
                    result.subtractRevenue(revenue);
                    break;
                default:
                    LOG.warn("不支持的聚合操作类型: {}", lineItem.getOperationType());
                    return;
            }

            resultState.update(result);
            LOG.info("聚合结果更新: {}", result);
            out.collect(result.toString());
        } catch (Exception e) {
            LOG.error("聚合计算异常", e);
            throw new DataProcessException("聚合计算异常", e);
        }
    }
}