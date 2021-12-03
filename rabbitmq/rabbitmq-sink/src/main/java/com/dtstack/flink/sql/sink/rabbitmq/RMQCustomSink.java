package com.dtstack.flink.sql.sink.rabbitmq;

import com.dtstack.flink.sql.format.SerializationMetricWrapper;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

public class RMQCustomSink<IN> extends RMQSink<IN> {

    private SerializationMetricWrapper serializationSchema;

    public RMQCustomSink(RMQConnectionConfig rmqConnectionConfig, String queueName, SerializationSchema<IN> schema) {
        super(rmqConnectionConfig, queueName, schema);
        this.serializationSchema = (SerializationMetricWrapper) schema;
    }

    @Override
    public void open(Configuration config) throws Exception {

        this.serializationSchema.setRuntimeContext(this.getRuntimeContext());
        this.serializationSchema.initMetric();
        super.open(config);
    }
}
