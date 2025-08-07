package cn.wxxlamp.ust.hk.entity;

import cn.wxxlamp.ust.hk.constant.TpcHConstants;
import cn.wxxlamp.ust.hk.exception.DataFormatException;

import java.time.LocalDate;

/**
 * Entity class representing an Order in the TPC-H dataset.
 * This class extends BaseEntity and provides specific functionality
 * for handling order data including date filtering.
 *
 * @author wxx
 * @version 2025-08-03 17:07
 */
public class Orders extends BaseEntity {
    /**
     * Default constructor
     */
    public Orders() {
        super();
    }

    /**
     * Constructor that parses raw field data
     * @param rawFields Array of raw field strings to parse
     */
    public Orders(String[] rawFields) {
        super(rawFields);
    }

    /**
     * Parse raw field data into order-specific fields
     * @param rawFields Array of raw field strings to parse
     */
    @Override
    protected void parseRawFields(String[] rawFields) {
        try {
            // Step 1: Extract order fields from raw data
            // rawFields format: [table_name, o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, ...]
            setFieldValue(TpcHConstants.FIELD_O_ORDERKEY, rawFields[1]);
            setFieldValue(TpcHConstants.FIELD_O_CUSTKEY, rawFields[2]);
            setFieldValue(TpcHConstants.FIELD_O_ORDERDATE, rawFields[5]);
            setFieldValue(TpcHConstants.FIELD_O_SHIPPRIORITY, rawFields[8]);
            
            // Step 2: Set the key value for grouping operations (using customer key)
            setKeyValue(getFieldValue(TpcHConstants.FIELD_O_CUSTKEY));
        } catch (Exception e) {
            throw new DataFormatException("Failed to parse order data", e);
        }
    }

    /**
     * Check if the order date is before the threshold date
     * @return true if the order date is before the threshold, false otherwise
     */
    public boolean isOrderDateBeforeThreshold() {
        try {
            String orderDate = getFieldValue(TpcHConstants.FIELD_O_ORDERDATE);
            return LocalDate.parse(orderDate).isBefore(
                    LocalDate.parse(TpcHConstants.ORDER_DATE_THRESHOLD));
        } catch (Exception e) {
            throw new DataFormatException("Failed to parse order date", e);
        }
    }
}