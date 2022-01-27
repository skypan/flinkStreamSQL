package com.dtstack.flink.sql.sink.dorisdb;

import com.dtstack.flink.sql.sink.IStreamSinkGener;
import com.dtstack.flink.sql.sink.dorisdb.builder.CustomDorisSinkRowBuilder;
import com.dtstack.flink.sql.sink.dorisdb.table.DorisdbSinkOptions;
import com.dtstack.flink.sql.sink.dorisdb.table.DorisdbTableInfo;
import com.dtstack.flink.sql.sink.rdb.AbstractRdbSink;
import com.dtstack.flink.sql.sink.rdb.JDBCOptions;
import com.dtstack.flink.sql.sink.rdb.format.JDBCUpsertOutputFormat;
import com.dtstack.flink.sql.table.AbstractTargetTableInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;

/**
 * DorisDB Sink
 */
public class DorisdbSink extends AbstractRdbSink implements IStreamSinkGener<AbstractRdbSink> {

    private String registerTabName;

    private int parallelism = 1;

    private DorisdbTableInfo dorisdbTableInfo;

    public DorisdbSink() {
        super(new DorisdbDialect());
    }

    @Override
    public JDBCUpsertOutputFormat getOutputFormat() {
        JDBCOptions jdbcOptions = JDBCOptions.builder()
                .setDbUrl(dbUrl)
                .setDialect(jdbcDialect)
                .setUsername(userName)
                .setPassword(password)
                .setTableName(tableName)
                .build();

        return JDBCUpsertOutputFormat.builder()
                .setOptions(jdbcOptions)
                .setFieldNames(fieldNames)
                .setFlushMaxSize(batchNum)
                .setFlushIntervalMills(batchWaitInterval)
                .setFieldTypes(sqlTypes)
                .setKeyFields(primaryKeys)
                .setAllReplace(allReplace)
                .setUpdateMode(updateMode)
                .build();
    }

    @Override
    public DorisdbSink genStreamSink(AbstractTargetTableInfo targetTableInfo) {
        dorisdbTableInfo = (DorisdbTableInfo) targetTableInfo;
        this.registerTabName = dorisdbTableInfo.getName();
        this.parallelism = dorisdbTableInfo.getParallelism();
        super.genStreamSink(targetTableInfo);
        return this;
    }


    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        // DorisSink
        SinkFunction<Tuple2<Boolean, Row>> sinkFunction = DorisSink.sink(getTableSchema(), buildDorisSinkOptions(), new CustomDorisSinkRowBuilder(getFieldNames(),getTableSchema().getFieldDataTypes()));
        DataStreamSink<Tuple2<Boolean, Row>> streamSink = dataStream.addSink(sinkFunction).setParallelism(this.parallelism).name(registerTabName);
        return streamSink;
    }

    private DorisdbSinkOptions buildDorisSinkOptions() {
        final DorisdbSinkOptions.Builder builder = DorisdbSinkOptions.builder();

        builder.withProperty("jdbc-url", dorisdbTableInfo.getJdbcUrl())
                .withProperty("load-url", dorisdbTableInfo.getLoadUrl())
                .withProperty("username", userName)
                .withProperty("password", password)
                .withProperty("table-name", tableName)
                .withProperty("database-name", dorisdbTableInfo.getDatabaseName());

        dorisdbTableInfo.getSinkProperties().forEach((key, value) -> {
            builder.withProperty(key, value);
        });

        return builder.build();
    }
}
