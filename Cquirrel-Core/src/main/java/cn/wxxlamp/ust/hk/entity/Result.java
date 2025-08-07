package cn.wxxlamp.ust.hk.entity;

import cn.wxxlamp.ust.hk.constant.TpcHConstants;

import java.math.BigDecimal;

/**
 * Result class representing the final output of the TPC-H Q3 query.
 * This class holds the aggregated results including order key, order date,
 * ship priority, and calculated revenue.
 *
 * @author wxx
 * @version 2025-08-03 17:09
 */
public class Result {
    /** The order key for this result */
    private String orderKey;
    
    /** The order date for this result */
    private String orderDate;
    
    /** The ship priority for this result */
    private String shipPriority;
    
    /** The calculated revenue for this result */
    private BigDecimal revenue;

    /**
     * Constructor that initializes the result from a base entity
     * @param entity The base entity to initialize from
     */
    public Result(BaseEntity entity) {
        // Step 1: Extract fields from the entity
        this.orderKey = entity.getFieldValue(TpcHConstants.FIELD_L_ORDERKEY);
        this.orderDate = entity.getFieldValue(TpcHConstants.FIELD_O_ORDERDATE);
        this.shipPriority = entity.getFieldValue(TpcHConstants.FIELD_O_SHIPPRIORITY);
        
        // Step 2: Initialize revenue to zero
        this.revenue = BigDecimal.ZERO;
    }

    /**
     * Get the order key
     * @return The order key
     */
    public String getOrderKey() {
        return orderKey;
    }

    /**
     * Set the order key
     * @param orderKey The order key to set
     */
    public void setOrderKey(String orderKey) {
        this.orderKey = orderKey;
    }

    /**
     * Get the order date
     * @return The order date
     */
    public String getOrderDate() {
        return orderDate;
    }

    /**
     * Set the order date
     * @param orderDate The order date to set
     */
    public void setOrderDate(String orderDate) {
        this.orderDate = orderDate;
    }

    /**
     * Get the ship priority
     * @return The ship priority
     */
    public String getShipPriority() {
        return shipPriority;
    }

    /**
     * Set the ship priority
     * @param shipPriority The ship priority to set
     */
    public void setShipPriority(String shipPriority) {
        this.shipPriority = shipPriority;
    }

    /**
     * Get the revenue
     * @return The revenue
     */
    public BigDecimal getRevenue() {
        return revenue;
    }

    /**
     * Add an amount to the revenue
     * @param amount The amount to add
     */
    public void addRevenue(BigDecimal amount) {
        this.revenue = this.revenue.add(amount);
    }

    /**
     * Subtract an amount from the revenue
     * @param amount The amount to subtract
     */
    public void subtractRevenue(BigDecimal amount) {
        this.revenue = this.revenue.subtract(amount);
    }

    @Override
    public String toString() {
        return String.format("%s|%s|%s|%.4f",
                orderKey, orderDate, shipPriority, revenue);
    }
}
