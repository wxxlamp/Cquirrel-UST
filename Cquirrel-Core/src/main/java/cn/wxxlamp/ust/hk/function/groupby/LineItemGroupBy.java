package cn.wxxlamp.ust.hk.function.groupby;

import cn.wxxlamp.ust.hk.entity.LineItem;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * @author wxx
 * @version 2025-08-03 22:57
 */
public class LineItemGroupBy implements KeySelector<LineItem, String> {
    @Override
    public String getKey(LineItem value) throws Exception {
        return value.getKeyValue();
    }
}
