package cn.wxxlamp.ust.hk.entity;

import java.util.HashMap;
import java.util.Objects;

/**
 * @author wxx
 * @version 19:54
 */
public class InnerMessage {
    public OperationType type;
    public String key_value;
    public HashMap<String, String> fields;

    public InnerMessage(String[] rawFields) {
        this.type = OperationType.INSERT;
        String tableName = rawFields[0];

        // initialize fields with fixed capacity, reduce hashmap resizing
        this.fields = new HashMap<>(70);

        switch (tableName) {
            case "customer":
                this.fields.put("c_custkey", rawFields[1]);
                this.fields.put("c_mktsegment", rawFields[7]); // 提取市场segment
                this.key_value = this.fields.get("c_custkey");
                break;
            case "orders":
                this.fields.put("o_orderkey", rawFields[1]);
                this.fields.put("o_custkey", rawFields[2]);
                this.fields.put("o_orderdate", rawFields[5]);
                this.fields.put("o_shippriority", rawFields[8]);
                this.key_value = this.fields.get("o_custkey"); // 关联c_custkey
                break;
            case "lineitem":
                this.fields.put("l_orderkey", rawFields[1]);
                this.fields.put("l_extendedprice", rawFields[6]);
                this.fields.put("l_discount", rawFields[7]);
                this.fields.put("l_shipdate", rawFields[11]);
                this.key_value = this.fields.get("l_orderkey"); // 关联o_orderkey
                break;
            default:
                throw new IllegalArgumentException("111");
        }

    }

    public InnerMessage(InnerMessage self, InnerMessage other) {
        this.type = self.type;
        this.key_value = self.key_value;
        this.fields = new HashMap<>(self.fields);
        this.fields.putAll(other.fields);
    }

    public void setType(OperationType type) {
        assert (type != this.type) : "Logic error: setting the same type";
        this.type = type;
    }

    /// Find the value in fields and set it as the key
    public void setKeyByKeyName(String keyName) {
        String value = this.fields.get(keyName);
        assert value != null : "Logic error: key not found in fields";
        this.key_value = value;
    }

    public void setGroupKeyByKeyNames(String keyNames[]) {
        String mixedValue = "";
        for (String keyName : keyNames) {
            String value = this.fields.get(keyName);
            mixedValue += value;
            assert value != null : "Logic error: key not found in fields";
        }
        this.key_value = mixedValue;
    }

    // get the value of the key, null if not found
    public String getValueByKey(String key) {
        return this.fields.get(key);
    }

    public enum OperationType {
        // input operations:
        INSERT,
        DELETE,
        // update operations:
        SET_ALIVE,
        SET_DEAD,
        SET_ALIVE_LEFT,
        SET_ALIVE_RIGHT,
        SET_DEAD_LEFT,
        SET_DEAD_RIGHT,
        AGGREGATE,
        AGGREGATE_DELETE
    }

    @Override
    public String toString() {
        String result = "InnerMessage{" +
                "type=" + type +
                ", key_value='" + key_value + '\'' +
                ", fields=" + fields.toString() + "}";
        return result;

    }

    // overide equals and hashcode to use InnerMessage as key in HashMap
    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof InnerMessage)) {
            return false;
        }
        InnerMessage other = (InnerMessage) obj;
        return this.fields.equals(other.fields);
    }


    @Override
    public int hashCode() {
        return Objects.hash(fields);
    }

}