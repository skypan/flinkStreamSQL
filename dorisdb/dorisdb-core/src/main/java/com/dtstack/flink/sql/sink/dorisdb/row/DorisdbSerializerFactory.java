package com.dtstack.flink.sql.sink.dorisdb.row;

import com.dtstack.flink.sql.sink.dorisdb.table.DorisdbSinkOptions;

public class DorisdbSerializerFactory {
    private DorisdbSerializerFactory() {
    }

    public static DorisdbISerializer createSerializer(DorisdbSinkOptions sinkOptions, String[] fieldNames) {
        if (DorisdbSinkOptions.StreamLoadFormat.CSV.equals(sinkOptions.getStreamLoadFormat())) {
            return new DorisdbCsvSerializer(sinkOptions.getSinkStreamLoadProperties().get("column_separator"));
        }
        if (DorisdbSinkOptions.StreamLoadFormat.JSON.equals(sinkOptions.getStreamLoadFormat())) {
            if (sinkOptions.supportUpsertDelete()) {
                String[] tmp = new String[fieldNames.length + 1];
                System.arraycopy(fieldNames, 0, tmp, 0, fieldNames.length);
                tmp[fieldNames.length] = DorisdbSinkOP.COLUMN_KEY;
                fieldNames = tmp;
            }
            return new DorisdbJsonSerializer(fieldNames);
        }
        throw new RuntimeException("Failed to create row serializer, unsupported `format` from stream load properties.");
    }
}
