package com.dtstack.flink.sql.sink.rabbitmq.table;

import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.table.AbstractTableParser;
import com.dtstack.flink.sql.util.MathUtil;

import java.util.Map;

public class RabbitmqSinkParser extends AbstractTableParser {
    @Override
    public AbstractTableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) throws Exception {
        RabbitmqSinkTableInfo sourceTableInfo = new RabbitmqSinkTableInfo();
        sourceTableInfo.setName(tableName);
        parseFieldsInfo(fieldsInfo, sourceTableInfo);

        sourceTableInfo.setParallelism(MathUtil.getIntegerVal(props.get(RabbitmqSinkTableInfo.PARALLELISM_KEY), 1));
        sourceTableInfo.setType(MathUtil.getString(props.get(RabbitmqSinkTableInfo.TYPE_KEY)));
        sourceTableInfo.setHost(MathUtil.getString(props.get(RabbitmqSinkTableInfo.HOST_KEY)));
        sourceTableInfo.setPort(MathUtil.getIntegerVal(props.get(RabbitmqSinkTableInfo.PORT_KEY), 15672));
        sourceTableInfo.setUsername(MathUtil.getString(props.get(RabbitmqSinkTableInfo.USERNAME_KEY)));
        sourceTableInfo.setPassword(MathUtil.getString(props.get(RabbitmqSinkTableInfo.PASSWORD_KEY)));
        sourceTableInfo.setQueue(MathUtil.getString(props.get(RabbitmqSinkTableInfo.QUEUE_KEY)));
        return sourceTableInfo;
    }
}
