package cn.wxxlamp.ust.hk.entity;

import cn.wxxlamp.ust.hk.constant.OperationType;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Abstract base class for all entities in the TPC-H Q3 processing pipeline.
 * This class provides common functionality for handling entity data including
 * operation type, key values, and field mappings.
 *
 * @author wxx
 * @version 2025-08-03 17:06
 */
public abstract class BaseEntity {
    /** The operation type for this entity (INSERT, DELETE, etc.) */
    protected OperationType operationType;
    
    /** The key value used for grouping and joining entities */
    protected String keyValue;
    
    /** Map of field names to their values */
    protected Map<String, String> fields;

    /**
     * Default constructor that initializes the fields map
     */
    public BaseEntity() {
        this.fields = new HashMap<>(32);
    }

    /**
     * Constructor that parses raw field data into the entity
     * @param rawFields Array of raw field strings to parse
     */
    public BaseEntity(String[] rawFields) {
        this();
        parseRawFields(rawFields);
    }

    /**
     * Abstract method to parse raw field data into the entity's fields
     * @param rawFields Array of raw field strings to parse
     */
    protected abstract void parseRawFields(String[] rawFields);

    /**
     * Get the operation type for this entity
     * @return The operation type
     */
    public OperationType getOperationType() {
        return operationType;
    }

    /**
     * Set the operation type for this entity
     * @param operationType The operation type to set
     */
    public void setOperationType(OperationType operationType) {
        this.operationType = operationType;
    }

    /**
     * Get the key value for this entity
     * @return The key value
     */
    public String getKeyValue() {
        return keyValue;
    }

    /**
     * Set the key value for this entity
     * @param keyValue The key value to set
     */
    public void setKeyValue(String keyValue) {
        this.keyValue = keyValue;
    }

    /**
     * Get all fields for this entity
     * @return Map of field names to values
     */
    public Map<String, String> getFields() {
        return fields;
    }

    /**
     * Get the value of a specific field
     * @param fieldName Name of the field to retrieve
     * @return Value of the field or empty string if not found
     */
    public String getFieldValue(String fieldName) {
        return fields.getOrDefault(fieldName, StringUtils.EMPTY);
    }

    /**
     * Set the value of a specific field
     * @param fieldName Name of the field to set
     * @param value Value to set for the field
     */
    public void setFieldValue(String fieldName, String value) {
        fields.put(fieldName, value);
    }

    /**
     * Merge fields from another entity into this one
     * @param other The other entity to merge fields from
     */
    public void mergeFields(BaseEntity other) {
        if (other != null && other.getFields() != null) {
            this.fields.putAll(other.getFields());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BaseEntity that = (BaseEntity) o;
        return Objects.equals(fields, that.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fields);
    }
}



