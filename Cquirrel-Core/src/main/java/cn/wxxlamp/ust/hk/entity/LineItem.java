package cn.wxxlamp.ust.hk.entity;

import cn.wxxlamp.ust.hk.constant.TpcHConstants;
import cn.wxxlamp.ust.hk.exception.DataFormatException;

import java.time.LocalDate;

/**
 * Entity class representing a LineItem in the TPC-H dataset.
 * This class extends BaseEntity and provides specific functionality
 * for handling line item data including ship date filtering and revenue calculation.
 *
 * @author wxx
 * @version 2025-08-03 17:08
 */
public class LineItem extends BaseEntity {
    /**
     * Default constructor
     */
    public LineItem() {
        super();
    }

    /**
     * Constructor that parses raw field data
     * @param rawFields Array of raw field strings to parse
     */
    public LineItem(String[] rawFields) {
        super(rawFields);
    }

    /**
     * Parse raw field data into line item-specific fields
     * @param rawFields Array of raw field strings to parse
     */
    @Override
    protected void parseRawFields(String[] rawFields) {
        try {
            // Step 1: Extract line item fields from raw data
            // rawFields format: [table_name, l_orderkey, l_partkey, l_suppkey, l_linenumber, ..., l_extendedprice, l_discount, ..., l_shipdate, ...]
            setFieldValue(TpcHConstants.FIELD_L_ORDERKEY, rawFields[1]);
            setFieldValue(TpcHConstants.FIELD_L_EXTENDEDPRICE, rawFields[6]);
            setFieldValue(TpcHConstants.FIELD_L_DISCOUNT, rawFields[7]);
            setFieldValue(TpcHConstants.FIELD_L_SHIPDATE, rawFields[11]);
            
            // Step 2: Set the key value for grouping operations (using order key)
            setKeyValue(getFieldValue(TpcHConstants.FIELD_L_ORDERKEY));
        } catch (Exception e) {
            throw new DataFormatException("Failed to parse line item data", e);
        }
    }

    /**
     * Check if the ship date is after the threshold date
     * @return true if the ship date is after the threshold, false otherwise
     */
    public boolean isShipDateAfterThreshold() {
        try {
            String shipDate = getFieldValue(TpcHConstants.FIELD_L_SHIPDATE);
            return LocalDate.parse(shipDate).isAfter(
                    LocalDate.parse(TpcHConstants.SHIP_DATE_THRESHOLD));
        } catch (Exception e) {
            throw new DataFormatException("Failed to parse ship date", e);
        }
    }

    /**
     * Calculate the revenue for this line item
     * @return The calculated revenue (extended price * (1 - discount))
     */
    public double calculateRevenue() {
        try {
            // Step 1: Parse extended price and discount values
            double extendedPrice = Double.parseDouble(
                    getFieldValue(TpcHConstants.FIELD_L_EXTENDEDPRICE));
            double discount = Double.parseDouble(
                    getFieldValue(TpcHConstants.FIELD_L_DISCOUNT));
            
            // Step 2: Calculate revenue using the formula: extendedPrice * (1 - discount)
            return extendedPrice * (1 - discount);
        } catch (Exception e) {
            throw new DataFormatException("Failed to calculate revenue", e);
        }
    }
}