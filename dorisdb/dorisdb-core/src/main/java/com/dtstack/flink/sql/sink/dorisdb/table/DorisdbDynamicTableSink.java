package com.dtstack.flink.sql.sink.dorisdb.table;

import com.dtstack.flink.sql.sink.dorisdb.row.DorisdbTableRowTransformer;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;

public class DorisdbDynamicTableSink implements DynamicTableSink {

    private transient TableSchema flinkSchema;
    private DorisdbSinkOptions sinkOptions;

    public DorisdbDynamicTableSink(DorisdbSinkOptions sinkOptions, TableSchema schema) {
        this.flinkSchema = schema;
        this.sinkOptions = sinkOptions;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return requestedMode;
    }

    @Override
    @SuppressWarnings("unchecked")
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final TypeInformation<RowData> rowDataTypeInfo = (TypeInformation<RowData>) context.createTypeInformation(flinkSchema.toRowDataType());
        DorisdbDynamicSinkFunction<RowData> DorisdbSinkFunction = new DorisdbDynamicSinkFunction<>(
                sinkOptions,
                flinkSchema,
                new DorisdbTableRowTransformer(rowDataTypeInfo)
        );
        return SinkFunctionProvider.of(DorisdbSinkFunction);
    }

    @Override
    public DynamicTableSink copy() {
        return new DorisdbDynamicTableSink(sinkOptions, flinkSchema);
    }

    @Override
    public String asSummaryString() {
        return "Dorisdb_sink";
    }
}
