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
 * Main class for executing the TPC-H Q3 query processing job.
 * This class sets up the Flink streaming environment, reads data from a file source,
 * processes it through various transformations, and outputs the results.
 *
 * Key steps:
 * 1. Initialize the Flink streaming environment
 * 2. Read data from a file source
 * 3. Split the data stream into separate streams for each entity (Customer, Orders, LineItem)
 * 4. Process and filter each entity stream
 * 5. Join the streams together based on key relationships
 * 6. Aggregate the joined data to calculate revenue
 * 7. Output the results to a sink
 *
 * @author wxx
 * @version 2025-08-03 17:21
 */
public class CquirrelJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(CquirrelJob.class);

    public static void main(String[] args) throws Exception {
        // Step 1: Initialize the Flink streaming environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Step 2: Create a file source to read data
        FileSource<String> source = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path(FilePathConstants.TBL_FILE_PATH))
                .build();
        DataStream<String> dataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "File Source");

        // Step 3: Split the data stream into separate streams for each entity
        SingleOutputStreamOperator<BaseEntity> mainDataStream = dataStream.process(new SplitStreamFunction())
                .name("Q3 Data Splitter")
                .setParallelism(1);

        // Step 4: Extract the individual entity streams from the split output
        DataStream<Customer> customerStream = mainDataStream.getSideOutput(SplitStreamFunction.CUSTOMER_TAG);
        DataStream<Orders> ordersStream = mainDataStream.getSideOutput(SplitStreamFunction.ORDERS_TAG);
        DataStream<LineItem> lineitemStream = mainDataStream.getSideOutput(SplitStreamFunction.LINEITEM_TAG);

        // Step 5: Initialize group-by functions for keying operations
        CustomerGroupBy customerGroupBy = new CustomerGroupBy();
        OrdersGroupBy ordersGroupBy = new OrdersGroupBy();

        // Step 6: Process customer data, filtering for automobile market customers
        DataStream<Customer> processedCustomerStream = customerStream
                .keyBy(customerGroupBy)
                .process(new CustomerProcessFunction())
                .name("Customer Processor");

        // Step 7: Join customer and orders data
        DataStream<Orders> processedOrdersStream = processedCustomerStream
                .connect(ordersStream)
                .keyBy(customerGroupBy, ordersGroupBy)
                .process(new OrdersProcessFunction())
                .name("Orders Processor");

        // Step 8: Join orders and line item data
        DataStream<LineItem> processedLineItemStream = processedOrdersStream
                .connect(lineitemStream)
                .keyBy(ordersGroupBy, new LineItemGroupBy())
                .process(new LineItemProcessFunction())
                .name("LineItem Processor");

        // Step 9: Aggregate the data to calculate revenue
        DataStream<Result> resultStream = processedLineItemStream
                .keyBy(BaseEntity::getKeyValue)
                .process(new AggregateProcessFunction())
                .name("Q3 Aggregator");

        // Step 10: Output the results to a sink
        resultStream.addSink(new RevenueAggregateSink())
                .name("Revenue Aggregate Sink");

        // Step 11: Execute the Flink job
        env.execute("TPC-H Q3 Query Processing");
        LOGGER.info("TPC-H Q3 query processing job completed successfully");
        AsyncFileWriterHandler.getAsyncFileWriter().stop();
    }
}
