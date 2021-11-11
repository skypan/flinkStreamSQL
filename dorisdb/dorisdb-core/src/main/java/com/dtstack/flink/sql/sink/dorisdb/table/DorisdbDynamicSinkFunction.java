package com.dtstack.flink.sql.sink.dorisdb.table;

import com.dtstack.flink.sql.sink.dorisdb.manager.DorisdbSinkManager;
import com.dtstack.flink.sql.sink.dorisdb.row.DorisdbIRowTransformer;
import com.dtstack.flink.sql.sink.dorisdb.row.DorisdbISerializer;
import com.dtstack.flink.sql.sink.dorisdb.row.DorisdbSerializerFactory;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.truncate.Truncate;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.calcite.shaded.com.google.common.base.Strings;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.NestedRowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.InstantiationUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DorisdbDynamicSinkFunction<T> extends RichSinkFunction<T> implements CheckpointedFunction {
    private static final long serialVersionUID = 1L;

    private DorisdbSinkManager sinkManager;
    private DorisdbIRowTransformer<T> rowTransformer;
    private DorisdbSinkOptions sinkOptions;
    private DorisdbISerializer serializer;
    private transient Counter totalInvokeRowsTime;
    private transient Counter totalInvokeRows;
    private static final String COUNTER_INVOKE_ROWS_COST_TIME = "totalInvokeRowsTimeNs";
    private static final String COUNTER_INVOKE_ROWS = "totalInvokeRows";

    // state only works with `DorisdbSinkSemantic.EXACTLY_ONCE`
    private transient ListState<Tuple2<String, List<byte[]>>> checkpointedState;

    public DorisdbDynamicSinkFunction(DorisdbSinkOptions sinkOptions, TableSchema schema, DorisdbIRowTransformer<T> rowTransformer) {
        this.sinkManager = new DorisdbSinkManager(sinkOptions, schema);
        rowTransformer.setTableSchema(schema);
        this.serializer = DorisdbSerializerFactory.createSerializer(sinkOptions, schema.getFieldNames());
        this.rowTransformer = rowTransformer;
        this.sinkOptions = sinkOptions;
    }

    public DorisdbDynamicSinkFunction(DorisdbSinkOptions sinkOptions) {
        this.sinkManager = new DorisdbSinkManager(sinkOptions, null);
        this.sinkOptions = sinkOptions;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        sinkManager.setRuntimeContext(getRuntimeContext());
        totalInvokeRows = getRuntimeContext().getMetricGroup().counter(COUNTER_INVOKE_ROWS);
        totalInvokeRowsTime = getRuntimeContext().getMetricGroup().counter(COUNTER_INVOKE_ROWS_COST_TIME);
        if (null != rowTransformer) {
            rowTransformer.setRuntimeContext(getRuntimeContext());
        }
        sinkManager.startScheduler();
        sinkManager.startAsyncFlushing();
    }

    @Override
    public synchronized void invoke(T value, Context context) throws Exception {
        long start = System.nanoTime();
        if (DorisdbSinkSemantic.EXACTLY_ONCE.equals(sinkOptions.getSemantic())) {
            // flush the batch saved at last checkpoint
            for (Tuple2<String, List<byte[]>> state : checkpointedState.get()) {
                sinkManager.setBufferedBatchList(state.f1);
                sinkManager.flush(state.f0, true);
            }
            checkpointedState.clear();
        }
        if (null == serializer) {
            // raw data sink
            sinkManager.writeRecord((String)value);
            totalInvokeRows.inc(1);
            totalInvokeRowsTime.inc(System.nanoTime() - start);
            return;
        }
        if (value instanceof RowData && !sinkOptions.supportUpsertDelete() && !RowKind.INSERT.equals(((RowData)value).getRowKind())) {
            // only primary key table support `update` and `delete`
            return;
        }
        if (value instanceof NestedRowData) {
            final int headerSize = 256;
            NestedRowData ddlData = (NestedRowData) value;
            if (ddlData.getSegments().length != 1 || ddlData.getSegments()[0].size() < headerSize) {
                return;
            }
            int totalSize = ddlData.getSegments()[0].size();
            byte[] data = new byte[totalSize - headerSize];
            ddlData.getSegments()[0].get(headerSize, data);
            Map<String, String> ddlMap = InstantiationUtil.deserializeObject(data, HashMap.class.getClassLoader());
            if (null == ddlMap
                    || "true".equals(ddlMap.get("snapshot"))
                    || Strings.isNullOrEmpty(ddlMap.get("ddl"))
                    || Strings.isNullOrEmpty(ddlMap.get("databaseName"))) {
                return;
            }
            Statement stmt = CCJSqlParserUtil.parse(ddlMap.get("ddl"));
            if (stmt instanceof Truncate) {
                Truncate truncate = (Truncate) stmt;
                if (!sinkOptions.getTableName().equalsIgnoreCase(truncate.getTable().getName())) {
                    return;
                }
                // TODO: add ddl to queue
            } else if (stmt instanceof Alter) {
                Alter alter = (Alter) stmt;
            }
        }
        sinkManager.writeRecord(
                serializer.serialize(rowTransformer.transform(value, sinkOptions.supportUpsertDelete()))
        );
        totalInvokeRows.inc(1);
        totalInvokeRowsTime.inc(System.nanoTime() - start);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        if (!DorisdbSinkSemantic.EXACTLY_ONCE.equals(sinkOptions.getSemantic())) {
            return;
        }
        ListStateDescriptor<Tuple2<String, List<byte[]>>> descriptor =
                new ListStateDescriptor<>(
                        "buffered-rows",
                        TypeInformation.of(new TypeHint<Tuple2<String, List<byte[]>>>(){})
                );
        checkpointedState = context.getOperatorStateStore().getListState(descriptor);
    }

    @Override
    public synchronized void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (DorisdbSinkSemantic.EXACTLY_ONCE.equals(sinkOptions.getSemantic())) {
            // save state
            checkpointedState.clear();
            checkpointedState.add(new Tuple2<>(sinkManager.createBatchLabel(), new ArrayList<>(sinkManager.getBufferedBatchList())));
            return;
        }
        sinkManager.flush(sinkManager.createBatchLabel(), true);
    }

    @Override
    public void close() throws Exception {
        sinkManager.close();
        super.close();
    }
}
