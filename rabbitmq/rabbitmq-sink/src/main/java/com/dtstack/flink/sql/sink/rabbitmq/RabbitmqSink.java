package com.dtstack.flink.sql.sink.rabbitmq;

import com.dtstack.flink.sql.format.SerializationMetricWrapper;
import com.dtstack.flink.sql.sink.IStreamSinkGener;
import com.dtstack.flink.sql.sink.rabbitmq.table.RabbitmqSinkTableInfo;
import com.dtstack.flink.sql.table.AbstractTargetTableInfo;
import com.dtstack.flink.sql.util.DataTypeUtils;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.stream.IntStream;

import static org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType;

public class RabbitmqSink implements RetractStreamTableSink<Row>, IStreamSinkGener {

    public static final String SINK_OPERATOR_NAME_TPL = "${queue}_${table}";

    protected String sinkOperatorName;

    protected String[] fieldNames;

    protected TypeInformation<?>[] fieldTypes;

    protected int parallelism = -1;

    protected String tableName;

    protected String queueName;

    protected String host;

    protected int port;

    protected String username;

    protected String password;

    protected TableSchema schema;

    protected SerializationMetricWrapper serializationSchema;

    @Override
    public RabbitmqSink genStreamSink(AbstractTargetTableInfo targetTableInfo) {
        RabbitmqSinkTableInfo sinkTableInfo = (RabbitmqSinkTableInfo) targetTableInfo;
        this.parallelism = sinkTableInfo.getParallelism();
        this.tableName = sinkTableInfo.getName();
        this.queueName = sinkTableInfo.getQueue();
        this.host = sinkTableInfo.getHost();
        this.port = sinkTableInfo.getPort();
        this.username = sinkTableInfo.getUsername();
        this.password = sinkTableInfo.getPassword();
        this.fieldNames = sinkTableInfo.getFields();
        this.fieldTypes = getTypeInformations(sinkTableInfo);
        this.schema = buildTableSchema(this.fieldNames, this.fieldTypes);
        this.sinkOperatorName = SINK_OPERATOR_NAME_TPL.replace("${queue}", this.queueName).replace("${table}", this.tableName);
        this.serializationSchema = new RabbitmqProducerFactory().createSerializationMetricWrapper(sinkTableInfo, this.getOutputType());
        return this;
    }

    @Override
    public TypeInformation<Row> getRecordType() {
        return new RowTypeInfo(fieldTypes, fieldNames);
    }

    @Override
    public DataStreamSink<Tuple2<Boolean, Row>> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {

        RMQConnectionConfig.Builder builder = new RMQConnectionConfig.Builder();
        RMQConnectionConfig config = builder
                .setHost(this.host)
                .setPort(this.port)
                .setUserName(this.username)
                .setPassword(this.password)
                .setVirtualHost("/").build();

        // 反序列化
        RMQSink<Tuple2<Boolean, Row>> rmqSink = new RMQSink<>(config, this.queueName, this.serializationSchema);

        DataStreamSink<Tuple2<Boolean, Row>> streamSink = dataStream.addSink(rmqSink);
        if (parallelism > 0) {
            streamSink.setParallelism(parallelism);
        }
        return streamSink;
    }

    // TODO Source有相同的方法日后可以合并
    protected TypeInformation[] getTypeInformations(RabbitmqSinkTableInfo sinkTableInfo) {
        String[] fieldTypes = sinkTableInfo.getFieldTypes();
        Class<?>[] fieldClasses = sinkTableInfo.getFieldClasses();
        TypeInformation[] types = IntStream.range(0, fieldClasses.length)
                .mapToObj(
                        i -> {
                            if (fieldClasses[i].isArray()) {
                                return DataTypeUtils.convertToArray(fieldTypes[i]);
                            }
                            if (fieldClasses[i] == new HashMap().getClass()) {
                                return DataTypeUtils.convertToMap(fieldTypes[i]);
                            }
                            return TypeInformation.of(fieldClasses[i]);
                        })
                .toArray(TypeInformation[]::new);
        return types;
    }

    protected TableSchema buildTableSchema(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        Preconditions.checkArgument(fieldNames.length == fieldTypes.length, "fieldNames length must equals fieldTypes length !");

        DataType[] dataTypes = IntStream.range(0, fieldTypes.length)
                .mapToObj(i -> fromLegacyInfoToDataType(fieldTypes[i]))
                .toArray(DataType[]::new);

        TableSchema tableSchema = TableSchema.builder()
                .fields(fieldNames, dataTypes)
                .build();
        return tableSchema;
    }

    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        return this;
    }

    @Override
    public TupleTypeInfo<Tuple2<Boolean, Row>> getOutputType() {
        return new TupleTypeInfo(org.apache.flink.table.api.Types.BOOLEAN(), new RowTypeInfo(fieldTypes, fieldNames));
    }

    @Override
    public TableSchema getTableSchema() {
        return schema;
    }
}
