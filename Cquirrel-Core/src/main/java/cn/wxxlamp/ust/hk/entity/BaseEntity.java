package cn.wxxlamp.ust.hk.entity;

import cn.wxxlamp.ust.hk.constant.OperationType;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * @author wxx
 * @version 2025-08-03 17:06
 */
public abstract class BaseEntity {
    protected OperationType operationType;
    protected String keyValue;
    protected Map<String, String> fields;

    public BaseEntity() {
        this.fields = new HashMap<>(32);
    }

    public BaseEntity(String[] rawFields) {
        this();
        parseRawFields(rawFields);
    }

    protected abstract void parseRawFields(String[] rawFields);

    public OperationType getOperationType() {
        return operationType;
    }

    public void setOperationType(OperationType operationType) {
        this.operationType = operationType;
    }

    public String getKeyValue() {
        return keyValue;
    }

    public void setKeyValue(String keyValue) {
        this.keyValue = keyValue;
    }

    public Map<String, String> getFields() {
        return fields;
    }

    public String getFieldValue(String fieldName) {
        return fields.getOrDefault(fieldName, StringUtils.EMPTY);
    }

    public void setFieldValue(String fieldName, String value) {
        fields.put(fieldName, value);
    }

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



