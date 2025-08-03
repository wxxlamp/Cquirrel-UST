package cn.wxxlamp.ust.hk.constant;

/**
 * @author wxx
 * @version 2025-08-03 17:03
 */
public class TpcHConstants {
    // 查询条件常量
    public static final String MKT_SEGMENT_AUTOMOBILE = "AUTOMOBILE";
    public static final String ORDER_DATE_THRESHOLD = "1995-03-13";
    public static final String SHIP_DATE_THRESHOLD = "1995-03-13";

    // 表名常量
    public static final String TABLE_CUSTOMER = "customer";
    public static final String TABLE_ORDERS = "orders";
    public static final String TABLE_LINEITEM = "lineitem";

    // 字段名常量
    public static final String FIELD_C_CUSTKEY = "c_custkey";
    public static final String FIELD_C_MKTSEGMENT = "c_mktsegment";
    public static final String FIELD_O_ORDERKEY = "o_orderkey";
    public static final String FIELD_O_CUSTKEY = "o_custkey";
    public static final String FIELD_O_ORDERDATE = "o_orderdate";
    public static final String FIELD_O_SHIPPRIORITY = "o_shippriority";
    public static final String FIELD_L_ORDERKEY = "l_orderkey";
    public static final String FIELD_L_EXTENDEDPRICE = "l_extendedprice";
    public static final String FIELD_L_DISCOUNT = "l_discount";
    public static final String FIELD_L_SHIPDATE = "l_shipdate";

    // 操作类型常量
    public static final String OPERATION_INSERT = "INSERT";
    public static final String OPERATION_DELETE = "DELETE";
}
