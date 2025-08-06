package cn.wxxlamp.ust.hk;

import cn.wxxlamp.ust.hk.constant.FilePathConstants;
import cn.wxxlamp.ust.hk.entity.*;
import cn.wxxlamp.ust.hk.function.aggregate.AggregateProcessFunction;
import cn.wxxlamp.ust.hk.function.input.SplitStreamFunction;
import cn.wxxlamp.ust.hk.function.entity.*;
import cn.wxxlamp.ust.hk.function.groupby.CustomerGroupBy;
import cn.wxxlamp.ust.hk.function.groupby.LineItemGroupBy;
import cn.wxxlamp.ust.hk.function.groupby.OrdersGroupBy;
import cn.wxxlamp.ust.hk.function.sink.AsyncFileWriterHandler;
import cn.wxxlamp.ust.hk.function.sink.RevenueAggregateSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wxx
 * @version 2025-08-03 17:21
 */
public class CquirrelJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(CquirrelJob.class);

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        FileSource<String> source = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path(FilePathConstants.TBL_FILE_PATH))
                .build();
        DataStream<String> dataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "File Source");

        // 拆分数据流
        SingleOutputStreamOperator<BaseEntity> mainDataStream = dataStream.process(new SplitStreamFunction())
                .name("Q3 Data Splitter")
                .setParallelism(1);

        // 获取各表的数据流
        DataStream<Customer> customerStream = mainDataStream.getSideOutput(SplitStreamFunction.CUSTOMER_TAG);
        DataStream<Orders> ordersStream = mainDataStream.getSideOutput(SplitStreamFunction.ORDERS_TAG);
        DataStream<LineItem> lineitemStream = mainDataStream.getSideOutput(SplitStreamFunction.LINEITEM_TAG);

        CustomerGroupBy customerGroupBy = new CustomerGroupBy();
        OrdersGroupBy ordersGroupBy = new OrdersGroupBy();
        // 处理客户数据，过滤汽车市场客户
        DataStream<Customer> processedCustomerStream = customerStream
                .keyBy(customerGroupBy)
                .process(new CustomerProcessFunction())
                .name("Customer Processor");

        // 关联客户和订单
        DataStream<Orders> processedOrdersStream = processedCustomerStream
                .connect(ordersStream)
                .keyBy(customerGroupBy, ordersGroupBy)
                .process(new OrdersProcessFunction())
                .name("Orders Processor");

        // 关联订单和订单项
        DataStream<LineItem> processedLineItemStream = processedOrdersStream
                .connect(lineitemStream)
                .keyBy(ordersGroupBy, new LineItemGroupBy())
                .process(new LineItemProcessFunction())
                .name("LineItem Processor");

        // 聚合计算
        DataStream<Result> resultStream = processedLineItemStream
                .keyBy(BaseEntity::getKeyValue)
                .process(new AggregateProcessFunction())
                .name("Q3 Aggregator");

        // print the results to file
        resultStream.addSink(new RevenueAggregateSink())
                .name("Revenue Aggregate Sink");

        // 执行作业
        env.execute("TPC-H Q3 Query Processing");
        LOGGER.info("TPC-H Q3 query task finished.");
        AsyncFileWriterHandler.getAsyncFileWriter().stop();
    }
}
