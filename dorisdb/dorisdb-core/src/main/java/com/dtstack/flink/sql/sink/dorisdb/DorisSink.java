package com.dtstack.flink.sql.sink.dorisdb;

import com.dtstack.flink.sql.sink.dorisdb.row.DorisdbGenericRowTransformer;
import com.dtstack.flink.sql.sink.dorisdb.row.DorisdbSinkRowBuilder;
import com.dtstack.flink.sql.sink.dorisdb.table.DorisdbDynamicSinkFunction;
import com.dtstack.flink.sql.sink.dorisdb.table.DorisdbSinkOptions;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.TableSchema;

public class DorisSink {
    /**
     * Create a Dorisdb DataStream sink.
     * <p>
     * Note: the objects passed to the return sink can be processed in batch and retried.
     * Therefore, objects can not be {@link org.apache.flink.api.common.ExecutionConfig#enableObjectReuse() reused}.
     * </p>
     *
     * @param flinkTableSchema TableSchema of the all columns with DataType
     * @param sinkOptions      DorisdbSinkOptions as the document listed, such as jdbc-url, load-url, batch size and maximum retries
     * @param <T>              type of data in {@link org.apache.flink.streaming.runtime.streamrecord.StreamRecord StreamRecord}.
     */
    public static <T> SinkFunction<T> sink(
            TableSchema flinkTableSchema,
            DorisdbSinkOptions sinkOptions,
            DorisdbSinkRowBuilder<T> rowDataTransformer) {
        return new DorisdbDynamicSinkFunction<>(
                sinkOptions,
                flinkTableSchema,
                new DorisdbGenericRowTransformer<>(rowDataTransformer)
        );
    }

    /**
     * Create a Dorisdb DataStream sink, stream elements could only be String.
     * <p>
     * Note: the objects passed to the return sink can be processed in batch and retried.
     * Therefore, objects can not be {@link org.apache.flink.api.common.ExecutionConfig#enableObjectReuse() reused}.
     * </p>
     *
     * @param sinkOptions DorisdbSinkOptions as the document listed, such as jdbc-url, load-url, batch size and maximum retries
     */
    public static SinkFunction<String> sink(DorisdbSinkOptions sinkOptions) {
        return new DorisdbDynamicSinkFunction<>(sinkOptions);
    }

    private DorisSink() {
    }
}
