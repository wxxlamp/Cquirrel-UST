package cn.wxxlamp.ust.hk.entity;

import cn.wxxlamp.ust.hk.constant.TpcHConstants;
import cn.wxxlamp.ust.hk.exception.DataFormatException;

/**
 * @author wxx
 * @version 2025-08-03 17:06
 */
public class Customer extends BaseEntity {
    public Customer() {
        super();
    }

    public Customer(String[] rawFields) {
        super(rawFields);
    }

    @Override
    protected void parseRawFields(String[] rawFields) {
        try {
            // rawFields格式: [表名, c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment]
            setFieldValue(TpcHConstants.FIELD_C_CUSTKEY, rawFields[1]);
            setFieldValue(TpcHConstants.FIELD_C_MKTSEGMENT, rawFields[7]);
            setKeyValue(getFieldValue(TpcHConstants.FIELD_C_CUSTKEY));
        } catch (Exception e) {
            throw new DataFormatException("解析客户数据失败", e);
        }
    }

    public boolean isAutomobileSegment() {
        return TpcHConstants.MKT_SEGMENT_AUTOMOBILE.equals(
                getFieldValue(TpcHConstants.FIELD_C_MKTSEGMENT));
    }
}
