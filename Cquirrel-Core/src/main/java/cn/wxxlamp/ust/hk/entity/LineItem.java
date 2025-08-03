package cn.wxxlamp.ust.hk.entity;

import cn.wxxlamp.ust.hk.constant.TpcHConstants;
import cn.wxxlamp.ust.hk.exception.DataFormatException;

import java.time.LocalDate;

/**
 * @author wxx
 * @version 2025-08-03 17:08
 */
public class LineItem extends BaseEntity {
    public LineItem() {
        super();
    }

    public LineItem(String[] rawFields) {
        super(rawFields);
    }

    @Override
    protected void parseRawFields(String[] rawFields) {
        try {
            // rawFields格式: [表名, l_orderkey, l_partkey, l_suppkey, l_linenumber, ..., l_extendedprice, l_discount, ..., l_shipdate, ...]
            setFieldValue(TpcHConstants.FIELD_L_ORDERKEY, rawFields[1]);
            setFieldValue(TpcHConstants.FIELD_L_EXTENDEDPRICE, rawFields[6]);
            setFieldValue(TpcHConstants.FIELD_L_DISCOUNT, rawFields[7]);
            setFieldValue(TpcHConstants.FIELD_L_SHIPDATE, rawFields[11]);
            setKeyValue(getFieldValue(TpcHConstants.FIELD_L_ORDERKEY));
        } catch (Exception e) {
            throw new DataFormatException("解析订单项数据失败", e);
        }
    }

    public boolean isShipDateAfterThreshold() {
        try {
            String shipDate = getFieldValue(TpcHConstants.FIELD_L_SHIPDATE);
            return LocalDate.parse(shipDate).isAfter(
                    LocalDate.parse(TpcHConstants.SHIP_DATE_THRESHOLD));
        } catch (Exception e) {
            throw new DataFormatException("解析发货日期失败", e);
        }
    }

    public double calculateRevenue() {
        try {
            double extendedPrice = Double.parseDouble(
                    getFieldValue(TpcHConstants.FIELD_L_EXTENDEDPRICE));
            double discount = Double.parseDouble(
                    getFieldValue(TpcHConstants.FIELD_L_DISCOUNT));
            return extendedPrice * (1 - discount);
        } catch (Exception e) {
            throw new DataFormatException("计算收入失败", e);
        }
    }
}