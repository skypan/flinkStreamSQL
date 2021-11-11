package com.dtstack.flink.sql.sink.dorisdb.row;

import com.alibaba.fastjson.JSON;

import java.util.List;
import java.util.Map;

public class DorisdbCsvSerializer implements DorisdbISerializer{
    private static final long serialVersionUID = 1L;

    private final String columnSeparator;

    public DorisdbCsvSerializer(String sp) {
        this.columnSeparator = DorisdbDelimiterParser.parse(sp, "\t");
    }

    @Override
    public String serialize(Object[] values) {
        StringBuilder sb = new StringBuilder();
        int idx = 0;
        for (Object val : values) {
            sb.append(null == val ? "\\N" : ((val instanceof Map || val instanceof List) ? JSON.toJSONString(val) : val));
            if (idx++ < values.length - 1) {
                sb.append(columnSeparator);
            }
        }
        return sb.toString();
    }
}
