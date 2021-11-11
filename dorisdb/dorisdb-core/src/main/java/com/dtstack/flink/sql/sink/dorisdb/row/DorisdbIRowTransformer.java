package com.dtstack.flink.sql.sink.dorisdb.row;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.api.TableSchema;

import java.io.Serializable;

public interface DorisdbIRowTransformer<T> extends Serializable {
    void setTableSchema(TableSchema tableSchema);

    void setRuntimeContext(RuntimeContext ctx);

    Object[] transform(T record, boolean supportUpsertDelete);
}
