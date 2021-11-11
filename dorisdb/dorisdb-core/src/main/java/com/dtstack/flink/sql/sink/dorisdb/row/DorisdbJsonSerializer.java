package com.dtstack.flink.sql.sink.dorisdb.row;

import com.alibaba.fastjson.JSON;

import java.util.HashMap;
import java.util.Map;

public class DorisdbJsonSerializer implements DorisdbISerializer{
    private static final long serialVersionUID = 1L;

    private final String[] fieldNames;

    public DorisdbJsonSerializer(String[] fieldNames) {
        this.fieldNames = fieldNames;
    }

    @Override
    public String serialize(Object[] values) {
        Map<String, Object> rowMap = new HashMap<>(values.length);
        int idx = 0;
        for (String fieldName : fieldNames) {
            rowMap.put(fieldName, values[idx] instanceof Map ? JSON.toJSONString(values[idx]) : values[idx]);
            idx++;
        }
        return JSON.toJSONString(rowMap);
    }
}
