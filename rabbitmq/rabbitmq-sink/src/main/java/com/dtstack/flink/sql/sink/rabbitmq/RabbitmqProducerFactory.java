package com.dtstack.flink.sql.sink.rabbitmq;

import com.dtstack.flink.sql.enums.EUpdateMode;
import com.dtstack.flink.sql.format.FormatType;
import com.dtstack.flink.sql.format.SerializationMetricWrapper;
import com.dtstack.flink.sql.sink.kafka.serialization.JsonTupleSerializationSchema;
import com.dtstack.flink.sql.sink.rabbitmq.table.RabbitmqSinkTableInfo;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

public class RabbitmqProducerFactory {

    public SerializationMetricWrapper createSerializationMetricWrapper(RabbitmqSinkTableInfo sinkTableInfo, TypeInformation<Tuple2<Boolean, Row>> typeInformation) {
        SerializationSchema<Tuple2<Boolean,Row>> serializationSchema = createSerializationSchema(sinkTableInfo, typeInformation);
        SerializationMetricWrapper wrapper = new SerializationMetricWrapper(serializationSchema);
        return wrapper;
    }

    private SerializationSchema<Tuple2<Boolean,Row>> createSerializationSchema(RabbitmqSinkTableInfo sinkTableInfo, TypeInformation<Tuple2<Boolean,Row>> typeInformation) {
        SerializationSchema<Tuple2<Boolean,Row>> serializationSchema = null;
        if (FormatType.JSON.name().equalsIgnoreCase(sinkTableInfo.getSinkDataType())) {
            if (typeInformation != null && typeInformation.getArity() != 0) {
                serializationSchema = new JsonTupleSerializationSchema(typeInformation, EUpdateMode.APPEND.name());
            } else {
                throw new IllegalArgumentException("sinkDataType:" + FormatType.JSON.name() + " must set schemaString（JSON Schema）or TypeInformation<Row>");
            }
        }

        if (null == serializationSchema) {
            throw new UnsupportedOperationException("FormatType:" + sinkTableInfo.getSinkDataType());
        }

        return serializationSchema;
    }
}
