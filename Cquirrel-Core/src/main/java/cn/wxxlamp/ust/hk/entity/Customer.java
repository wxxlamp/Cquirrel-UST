package cn.wxxlamp.ust.hk.entity;

import cn.wxxlamp.ust.hk.constant.TpcHConstants;
import cn.wxxlamp.ust.hk.exception.DataFormatException;

/**
 * Entity class representing a Customer in the TPC-H dataset.
 * This class extends BaseEntity and provides specific functionality
 * for handling customer data including market segment filtering.
 *
 * @author wxx
 * @version 2025-08-03 17:06
 */
public class Customer extends BaseEntity {
    /**
     * Default constructor
     */
    public Customer() {
        super();
    }

    /**
     * Constructor that parses raw field data
     * @param rawFields Array of raw field strings to parse
     */
    public Customer(String[] rawFields) {
        super(rawFields);
    }

    /**
     * Parse raw field data into customer-specific fields
     * @param rawFields Array of raw field strings to parse
     */
    @Override
    protected void parseRawFields(String[] rawFields) {
        try {
            // Step 1: Extract customer key and market segment from raw fields
            // rawFields format: [table_name, c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment]
            setFieldValue(TpcHConstants.FIELD_C_CUSTKEY, rawFields[1]);
            setFieldValue(TpcHConstants.FIELD_C_MKTSEGMENT, rawFields[7]);
            
            // Step 2: Set the key value for grouping operations
            setKeyValue(getFieldValue(TpcHConstants.FIELD_C_CUSTKEY));
        } catch (Exception e) {
            throw new DataFormatException("Failed to parse customer data", e);
        }
    }

    /**
     * Check if this customer belongs to the automobile market segment
     * @return true if the customer is in the automobile segment, false otherwise
     */
    public boolean isAutomobileSegment() {
        return TpcHConstants.MKT_SEGMENT_AUTOMOBILE.equals(
                getFieldValue(TpcHConstants.FIELD_C_MKTSEGMENT));
    }
}
