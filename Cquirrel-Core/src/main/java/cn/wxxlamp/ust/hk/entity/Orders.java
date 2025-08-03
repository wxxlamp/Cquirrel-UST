package cn.wxxlamp.ust.hk.entity;

import cn.wxxlamp.ust.hk.constant.TpcHConstants;
import cn.wxxlamp.ust.hk.exception.DataFormatException;

import java.time.LocalDate;

/**
 * @author wxx
 * @version 2025-08-03 17:07
 */
public class Orders extends BaseEntity {
    public Orders() {
        super();
    }

    public Orders(String[] rawFields) {
        super(rawFields);
    }

    @Override
    protected void parseRawFields(String[] rawFields) {
        try {
            // rawFields格式: [表名, o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, ...]
            setFieldValue(TpcHConstants.FIELD_O_ORDERKEY, rawFields[1]);
            setFieldValue(TpcHConstants.FIELD_O_CUSTKEY, rawFields[2]);
            setFieldValue(TpcHConstants.FIELD_O_ORDERDATE, rawFields[5]);
            setFieldValue(TpcHConstants.FIELD_O_SHIPPRIORITY, rawFields[8]);
            setKeyValue(getFieldValue(TpcHConstants.FIELD_O_CUSTKEY));
        } catch (Exception e) {
            throw new DataFormatException("解析订单数据失败", e);
        }
    }

    public boolean isOrderDateBeforeThreshold() {
        try {
            String orderDate = getFieldValue(TpcHConstants.FIELD_O_ORDERDATE);
            return LocalDate.parse(orderDate).isBefore(
                    LocalDate.parse(TpcHConstants.ORDER_DATE_THRESHOLD));
        } catch (Exception e) {
            throw new DataFormatException("解析订单日期失败", e);
        }
    }
}