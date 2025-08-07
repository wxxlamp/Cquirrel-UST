package cn.wxxlamp.ust.hk.constant;

/**
 * Enum representing different operation types in the data processing pipeline.
 * These operations define how entities should be handled during stream processing.
 *
 * @author wxx
 * @version 2025-08-03 17:05
 */
public enum OperationType {
    /** Insert a new entity into the stream */
    INSERT,
    
    /** Delete an entity from the stream */
    DELETE,
    
    /** Mark an entity as alive (active) */
    SET_ALIVE,
    
    /** Mark an entity as dead (inactive) */
    SET_DEAD,
    
    /** Mark the left side of an entity as alive */
    SET_ALIVE_LEFT,
    
    /** Mark the right side of an entity as alive */
    SET_ALIVE_RIGHT,
    
    /** Mark the left side of an entity as dead */
    SET_DEAD_LEFT,
    
    /** Mark the right side of an entity as dead */
    SET_DEAD_RIGHT,
    
    /** Perform aggregation operation on entities */
    AGGREGATE,
    
    /** Delete aggregated results */
    AGGREGATE_DELETE
}
