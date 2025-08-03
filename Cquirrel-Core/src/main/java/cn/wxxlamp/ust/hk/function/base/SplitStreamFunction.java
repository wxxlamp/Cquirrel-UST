package cn.wxxlamp.ust.hk.function.base;

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
 * @author wxx
 * @version 2025-08-03 17:20
 */
public class SplitStreamFunction extends ProcessFunction<String, BaseEntity> {
    private static final Logger LOG = LoggerFactory.getLogger(SplitStreamFunction.class);

    // 定义输出标签
    public static final OutputTag<Customer> CUSTOMER_TAG = new OutputTag<Customer>("customer") {};
    public static final OutputTag<Orders> ORDERS_TAG = new OutputTag<Orders>("orders") {};
    public static final OutputTag<LineItem> LINEITEM_TAG = new OutputTag<LineItem>("lineitem") {};

    @Override
    public void processElement(String value, Context ctx, Collector<BaseEntity> out) throws Exception {
        try {
            if (value == null || value.trim().isEmpty()) {
                LOG.warn("忽略空数据行");
                return;
            }

            String[] values = value.split("\\|");
            if (values.length < 2) {
                throw new DataFormatException("数据格式错误，字段数量不足: " + value);
            }

            String operation = values[0];
            String tableName = values[1];

            // 创建对应的实体对象
            BaseEntity entity = createEntity(tableName, values);
            if (entity == null) {
                LOG.warn("不支持的表名: {}", tableName);
                return;
            }

            // 设置操作类型
            if (TpcHConstants.OPERATION_INSERT.equalsIgnoreCase(operation)) {
                entity.setOperationType(OperationType.INSERT);
            } else if (TpcHConstants.OPERATION_DELETE.equalsIgnoreCase(operation)) {
                entity.setOperationType(OperationType.DELETE);
            } else {
                throw new DataFormatException("不支持的操作类型: " + operation);
            }

            // 输出到对应的侧输出流
            if (entity instanceof Customer) {
                ctx.output(CUSTOMER_TAG, (Customer) entity);
            } else if (entity instanceof Orders) {
                ctx.output(ORDERS_TAG, (Orders) entity);
            } else if (entity instanceof LineItem) {
                ctx.output(LINEITEM_TAG, (LineItem) entity);
            }

            LOG.debug("拆分数据: 表={}, 操作={}", tableName, operation);
        } catch (Exception e) {
            LOG.error("数据拆分异常: " + value, e);
            throw new DataFormatException("数据拆分异常", e);
        }
    }

    /**
     * 根据表名创建对应的实体对象
     */
    private BaseEntity createEntity(String tableName, String[] values) {
        String[] entityFields = new String[values.length - 1];
        System.arraycopy(values, 1, entityFields, 0, entityFields.length);

        switch (tableName) {
            case TpcHConstants.TABLE_CUSTOMER:
                return new Customer(entityFields);
            case TpcHConstants.TABLE_ORDERS:
                return new Orders(entityFields);
            case TpcHConstants.TABLE_LINEITEM:
                return new LineItem(entityFields);
            default:
                return null;
        }
    }
}