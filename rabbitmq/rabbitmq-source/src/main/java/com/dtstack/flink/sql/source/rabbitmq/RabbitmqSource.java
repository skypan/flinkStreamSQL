package com.dtstack.flink.sql.source.rabbitmq;

import com.dtstack.flink.sql.source.IStreamSourceGener;
import com.dtstack.flink.sql.source.rabbitmq.table.RabbitmqSourceTableInfo;
import com.dtstack.flink.sql.table.AbstractSourceTableInfo;
import com.dtstack.flink.sql.util.DataTypeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.stream.IntStream;

public class RabbitmqSource implements IStreamSourceGener<Table> {
    @Override
    public Table genStreamSource(AbstractSourceTableInfo sourceTableInfo, StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {

        RabbitmqSourceTableInfo rabbitmqSourceTableInfo = (RabbitmqSourceTableInfo) sourceTableInfo;

        RMQConnectionConfig.Builder builder = new RMQConnectionConfig.Builder();
        RMQConnectionConfig config = builder
                .setHost(rabbitmqSourceTableInfo.getHost())
                .setPort(rabbitmqSourceTableInfo.getPort())
                .setUserName(rabbitmqSourceTableInfo.getUsername())
                .setPassword(rabbitmqSourceTableInfo.getPassword())
                .setVirtualHost("/")
                .build();

        TypeInformation<Row> typeInformation = getRowTypeInformation(rabbitmqSourceTableInfo);

        JsonRowDeserializationSchema schema = new JsonRowDeserializationSchema.Builder(typeInformation).build();
        RMQSource<Row> rmqSource = new RMQSource(config, rabbitmqSourceTableInfo.getQueue(), schema);

        DataStreamSource mqSource = env.addSource(rmqSource, typeInformation);

        String fields = StringUtils.join(rabbitmqSourceTableInfo.getFields(), ",");
        return tableEnv.fromDataStream(mqSource, fields);
    }

    protected TypeInformation<Row> getRowTypeInformation(RabbitmqSourceTableInfo sourceTableInfo) {
        String[] fieldTypes = sourceTableInfo.getFieldTypes();
        Class<?>[] fieldClasses = sourceTableInfo.getFieldClasses();
        TypeInformation[] types =
                IntStream.range(0, fieldClasses.length)
                        .mapToObj(i -> {
                            if (fieldClasses[i].isArray()) {
                                return DataTypeUtils.convertToArray(fieldTypes[i]);
                            }
                            return TypeInformation.of(fieldClasses[i]);
                        })
                        .toArray(TypeInformation[]::new);


        return new RowTypeInfo(types, sourceTableInfo.getFields());
    }
}
