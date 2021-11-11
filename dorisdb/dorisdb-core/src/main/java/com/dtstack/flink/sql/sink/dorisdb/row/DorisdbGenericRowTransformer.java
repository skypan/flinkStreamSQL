package com.dtstack.flink.sql.sink.dorisdb.row;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;

public class DorisdbGenericRowTransformer<T> implements DorisdbIRowTransformer<T> {
    private static final long serialVersionUID = 1L;

    private DorisdbSinkRowBuilder<T> consumer;
    private String[] fieldNames;

    public DorisdbGenericRowTransformer(DorisdbSinkRowBuilder<T> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void setTableSchema(TableSchema ts) {
        fieldNames = ts.getFieldNames();
    }

    @Override
    public void setRuntimeContext(RuntimeContext ctx) {
    }

    @Override
    public Object[] transform(T record, boolean supportUpsertDelete) {
        Object[] rowData = new Object[fieldNames.length + (supportUpsertDelete ? 1 : 0)];
        consumer.accept(rowData, record);
        if (supportUpsertDelete && (record instanceof RowData)) {
            // set `__op` column
            rowData[rowData.length - 1] = DorisdbSinkOP.parse(((RowData) record).getRowKind()).ordinal();
        }
        return rowData;
    }
}
