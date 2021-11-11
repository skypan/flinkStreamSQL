package com.dtstack.flink.sql.sink.dorisdb.row;

import org.apache.flink.types.RowKind;

public enum DorisdbSinkOP {
    UPSERT, DELETE;

    public static final String COLUMN_KEY = "__op";

    static DorisdbSinkOP parse(RowKind kind) {
        if (RowKind.INSERT.equals(kind) || RowKind.UPDATE_AFTER.equals(kind)) {
            return UPSERT;
        }
        if (RowKind.DELETE.equals(kind) || RowKind.UPDATE_BEFORE.equals(kind)) {
            return DELETE;
        }
        throw new RuntimeException("Unsupported row kind.");
    }
}
