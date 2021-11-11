package com.dtstack.flink.sql.sink.dorisdb.table;

import org.apache.flink.annotation.Internal;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Internal
public enum DorisdbSinkSemantic {
    EXACTLY_ONCE("exactly-once"), AT_LEAST_ONCE("at-least-once");

    private String name;

    DorisdbSinkSemantic(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static DorisdbSinkSemantic fromName(String n) {
        List<DorisdbSinkSemantic> rs = Arrays.stream(DorisdbSinkSemantic.values()).filter(v -> v.getName().equals(n))
                .collect(Collectors.toList());
        return rs.isEmpty() ? null : rs.get(0);
    }
}
