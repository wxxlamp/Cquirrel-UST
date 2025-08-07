package cn.wxxlamp.ust.hk.constant;

/**
 * Enum representing the names of database tables used in the TPC-H benchmark.
 * Each value corresponds to a specific table in the TPC-H dataset.
 *
 * @author wxx
 * @version 2025-08-03 17:06
 */
public enum TableName {
    /** Represents the Customer table in TPC-H dataset */
    CUSTOMER,
    
    /** Represents the Orders table in TPC-H dataset */
    ORDERS,
    
    /** Represents the LineItem table in TPC-H dataset */
    LINEITEM
}
