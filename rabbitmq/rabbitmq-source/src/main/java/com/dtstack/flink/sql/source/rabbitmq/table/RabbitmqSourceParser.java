package com.dtstack.flink.sql.source.rabbitmq.table;

import com.dtstack.flink.sql.table.AbstractSourceParser;
import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.util.MathUtil;

import java.util.Map;

/**
 * Rabbitmq解析类
 */
public class RabbitmqSourceParser extends AbstractSourceParser {
    @Override
    public AbstractTableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) throws Exception {
        RabbitmqSourceTableInfo sourceTableInfo = new RabbitmqSourceTableInfo();
        sourceTableInfo.setName(tableName);
        parseFieldsInfo(fieldsInfo, sourceTableInfo);

        sourceTableInfo.setParallelism(MathUtil.getIntegerVal(props.get(RabbitmqSourceTableInfo.PARALLELISM_KEY), 1));
        sourceTableInfo.setType(MathUtil.getString(props.get(RabbitmqSourceTableInfo.TYPE_KEY)));
        sourceTableInfo.setHost(MathUtil.getString(props.get(RabbitmqSourceTableInfo.HOST_KEY)));
        sourceTableInfo.setPort(MathUtil.getIntegerVal(props.get(RabbitmqSourceTableInfo.PORT_KEY), 15672));
        sourceTableInfo.setUsername(MathUtil.getString(props.get(RabbitmqSourceTableInfo.USERNAME_KEY)));
        sourceTableInfo.setPassword(MathUtil.getString(props.get(RabbitmqSourceTableInfo.PASSWORD_KEY)));
        sourceTableInfo.setQueue(MathUtil.getString(props.get(RabbitmqSourceTableInfo.QUEUE_KEY)));
        return sourceTableInfo;
    }
}
