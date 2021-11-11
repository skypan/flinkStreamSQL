package com.dtstack.flink.sql.sink.dorisdb.builder;

import com.dtstack.flink.sql.sink.dorisdb.row.DorisdbSinkRowBuilder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

public class CustomDorisSinkRowBuilder implements DorisdbSinkRowBuilder<Tuple2<Boolean, Row>> {

    private String[] columns;

    public CustomDorisSinkRowBuilder(String[] columns) {
        this.columns = columns;
    }

    @Override
    public void accept(Object[] values, Tuple2<Boolean, Row> tuple2) {
        Row row = tuple2.f1;
        int nlen = values.length;
        for (int i = 0; i < nlen; i++) {
            values[i] = row.getField(i);
        }
    }
}
