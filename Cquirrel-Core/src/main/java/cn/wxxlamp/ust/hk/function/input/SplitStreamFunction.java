package cn.wxxlamp.ust.hk.function.input;

import cn.wxxlamp.ust.hk.constant.OperationType;
import cn.wxxlamp.ust.hk.constant.TpcHConstants;
import cn.wxxlamp.ust.hk.entity.BaseEntity;
import cn.wxxlamp.ust.hk.entity.Customer;
import cn.wxxlamp.ust.hk.entity.LineItem;
import cn.wxxlamp.ust.hk.entity.Orders;
import cn.wxxlamp.ust.hk.exception.DataFormatException;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Process function for splitting the input data stream into separate streams for each entity type.
 * This function parses input strings and routes them to the appropriate output tags based on
 * table name and operation type.
 *
 * @author wxx
 * @version 2025-08-03 17:20
 */
public class SplitStreamFunction extends ProcessFunction<String, BaseEntity> {
    private static final Logger LOG = LoggerFactory.getLogger(SplitStreamFunction.class);

    // Define output tags for each entity type
    public static final OutputTag<Customer> CUSTOMER_TAG = new OutputTag<>("customer") {
    };
    public static final OutputTag<Orders> ORDERS_TAG = new OutputTag<>("orders") {
    };
    public static final OutputTag<LineItem> LINEITEM_TAG = new OutputTag<>("lineitem") {
    };

    @Override
    public void processElement(String value, Context ctx, Collector<BaseEntity> out) {
        try {
            // Step 1: Validate input
            if (value == null || value.trim().isEmpty()) {
                LOG.warn("Ignoring empty data row");
                return;
            }

            // Step 2: Split the input string into fields
            String[] values = value.split("\\|");
            if (values.length < 2) {
                throw new DataFormatException("Data format error, insufficient fields: " + value);
            }

            // Step 3: Extract operation and table name
            String operation = values[0];
            String tableName = values[1];

            // Step 4: Create the appropriate entity object
            BaseEntity entity = createEntity(tableName, values);
            if (entity == null) {
                LOG.warn("Unsupported table name: {}", tableName);
                return;
            }

            // Step 5: Set operation type based on input
            if (TpcHConstants.OPERATION_INSERT.equalsIgnoreCase(operation)) {
                entity.setOperationType(OperationType.INSERT);
            } else if (TpcHConstants.OPERATION_DELETE.equalsIgnoreCase(operation)) {
                entity.setOperationType(OperationType.DELETE);
            } else {
                throw new DataFormatException("Unsupported operation type: " + operation);
            }

            // Step 6: Route to appropriate output stream based on entity type
            if (entity instanceof Customer) {
                ctx.output(CUSTOMER_TAG, (Customer) entity);
            } else if (entity instanceof Orders) {
                ctx.output(ORDERS_TAG, (Orders) entity);
            } else if (entity instanceof LineItem) {
                ctx.output(LINEITEM_TAG, (LineItem) entity);
            }

            LOG.debug("Split data: Table={}, Operation={}", tableName, operation);
        } catch (Exception e) {
            LOG.error("Data splitting error: " + value, e);
            throw new DataFormatException("Data splitting error", e);
        }
    }

    /**
     * Create an entity object based on the table name
     * @param tableName The name of the table
     * @param values The array of field values
     * @return The created entity object or null if table name is not supported
     */
    private BaseEntity createEntity(String tableName, String[] values) {
        // Step 1: Prepare entity fields by removing the table name
        String[] entityFields = new String[values.length - 1];
        System.arraycopy(values, 1, entityFields, 0, entityFields.length);

        // Step 2: Create entity based on table name
        return switch (tableName) {
            case TpcHConstants.TABLE_CUSTOMER -> new Customer(entityFields);
            case TpcHConstants.TABLE_ORDERS -> new Orders(entityFields);
            case TpcHConstants.TABLE_LINEITEM -> new LineItem(entityFields);
            default -> null;
        };
    }
}