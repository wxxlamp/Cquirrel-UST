package cn.wxxlamp.ust.hk.entity;

import cn.wxxlamp.ust.hk.constant.TpcHConstants;

/**
 * @author wxx
 * @version 2025-08-03 17:09
 */
public class Result {
    private String orderKey;
    private String orderDate;
    private String shipPriority;
    private double revenue;

    public Result(BaseEntity entity) {
        this.orderKey = entity.getFieldValue(TpcHConstants.FIELD_L_ORDERKEY);
        this.orderDate = entity.getFieldValue(TpcHConstants.FIELD_O_ORDERDATE);
        this.shipPriority = entity.getFieldValue(TpcHConstants.FIELD_O_SHIPPRIORITY);
        this.revenue = 0.0;
    }

    public String getOrderKey() {
        return orderKey;
    }

    public void setOrderKey(String orderKey) {
        this.orderKey = orderKey;
    }

    public String getOrderDate() {
        return orderDate;
    }

    public void setOrderDate(String orderDate) {
        this.orderDate = orderDate;
    }

    public String getShipPriority() {
        return shipPriority;
    }

    public void setShipPriority(String shipPriority) {
        this.shipPriority = shipPriority;
    }

    public double getRevenue() {
        return revenue;
    }

    public void addRevenue(double amount) {
        this.revenue += amount;
    }

    public void subtractRevenue(double amount) {
        this.revenue -= amount;
    }

    @Override
    public String toString() {
        return String.format("%s|%s|%s|%.4f",
                orderKey, orderDate, shipPriority, revenue);
    }
}
