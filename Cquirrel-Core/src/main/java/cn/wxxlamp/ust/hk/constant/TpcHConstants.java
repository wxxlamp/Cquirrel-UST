package cn.wxxlamp.ust.hk.constant;

/**
 * Constants used throughout the TPC-H Q3 query processing application.
 * This class contains various constants related to query conditions, table names,
 * field names, and operation types used in the data processing pipeline.
 *
 * @author wxx
 * @version 2025-08-03 17:03
 */
public class TpcHConstants {
    // Query condition constants
    /** Market segment value for automobile customers */
    public static final String MKT_SEGMENT_AUTOMOBILE = "AUTOMOBILE";
    
    /** Order date threshold for filtering orders */
    public static final String ORDER_DATE_THRESHOLD = "1995-03-13";
    
    /** Ship date threshold for filtering line items */
    public static final String SHIP_DATE_THRESHOLD = "1995-03-13";

    // Table name constants
    /** Name of the customer table */
    public static final String TABLE_CUSTOMER = "customer";
    
    /** Name of the orders table */
    public static final String TABLE_ORDERS = "orders";
    
    /** Name of the lineitem table */
    public static final String TABLE_LINEITEM = "lineitem";

    // Field name constants
    /** Customer key field name */
    public static final String FIELD_C_CUSTKEY = "c_custkey";
    
    /** Market segment field name */
    public static final String FIELD_C_MKTSEGMENT = "c_mktsegment";
    
    /** Order key field name */
    public static final String FIELD_O_ORDERKEY = "o_orderkey";
    
    /** Customer key in orders table */
    public static final String FIELD_O_CUSTKEY = "o_custkey";
    
    /** Order date field name */
    public static final String FIELD_O_ORDERDATE = "o_orderdate";
    
    /** Ship priority field name */
    public static final String FIELD_O_SHIPPRIORITY = "o_shippriority";
    
    /** Order key in lineitem table */
    public static final String FIELD_L_ORDERKEY = "l_orderkey";
    
    /** Extended price field name */
    public static final String FIELD_L_EXTENDEDPRICE = "l_extendedprice";
    
    /** Discount field name */
    public static final String FIELD_L_DISCOUNT = "l_discount";
    
    /** Ship date field name */
    public static final String FIELD_L_SHIPDATE = "l_shipdate";

    // Operation type constants
    /** Insert operation identifier */
    public static final String OPERATION_INSERT = "INSERT";
    
    /** Delete operation identifier */
    public static final String OPERATION_DELETE = "DELETE";
}
