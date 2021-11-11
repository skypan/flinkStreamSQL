package com.dtstack.flink.sql.sink.dorisdb.table;

import com.dtstack.flink.sql.core.rdb.JdbcCheckKeys;
import com.dtstack.flink.sql.parser.CreateTableParser;
import com.dtstack.flink.sql.sink.rdb.table.RdbSinkParser;
import com.dtstack.flink.sql.sink.rdb.table.RdbTableInfo;
import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.table.AbstractTableInfoParser;
import com.dtstack.flink.sql.table.AbstractTableParser;
import com.dtstack.flink.sql.util.MathUtil;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

public class DorisdbSinkParser extends RdbSinkParser {

    private static final String CURR_TYPE = "dorisdb";

    @Override
    public AbstractTableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) {
        props.put(JdbcCheckKeys.DRIVER_NAME, "com.mysql.jdbc.Driver");
        DorisdbTableInfo rdbTableInfo = new DorisdbTableInfo();

        rdbTableInfo.setName(tableName);
        parseFieldsInfo(fieldsInfo, rdbTableInfo);

        rdbTableInfo.setParallelism(MathUtil.getIntegerVal(props.get(RdbTableInfo.PARALLELISM_KEY.toLowerCase())));
        rdbTableInfo.setUrl(MathUtil.getString(props.get(RdbTableInfo.URL_KEY.toLowerCase())));
        rdbTableInfo.setTableName(MathUtil.getString(props.get(RdbTableInfo.TABLE_NAME_KEY.toLowerCase())));
        rdbTableInfo.setUserName(MathUtil.getString(props.get(RdbTableInfo.USER_NAME_KEY.toLowerCase())));
        rdbTableInfo.setPassword(MathUtil.getString(props.get(RdbTableInfo.PASSWORD_KEY.toLowerCase())));
        rdbTableInfo.setBatchSize(MathUtil.getIntegerVal(props.get(RdbTableInfo.BATCH_SIZE_KEY.toLowerCase())));
        rdbTableInfo.setBatchWaitInterval(MathUtil.getLongVal(props.get(RdbTableInfo.BATCH_WAIT_INTERVAL_KEY.toLowerCase())));
        rdbTableInfo.setBufferSize(MathUtil.getString(props.get(RdbTableInfo.BUFFER_SIZE_KEY.toLowerCase())));
        rdbTableInfo.setFlushIntervalMs(MathUtil.getString(props.get(RdbTableInfo.FLUSH_INTERVALMS_KEY.toLowerCase())));
        rdbTableInfo.setSchema(MathUtil.getString(props.get(RdbTableInfo.SCHEMA_KEY.toLowerCase())));
        rdbTableInfo.setUpdateMode(MathUtil.getString(props.get(RdbTableInfo.UPDATE_KEY.toLowerCase())));
        rdbTableInfo.setAllReplace(MathUtil.getBoolean(props.get(RdbTableInfo.ALLREPLACE_KEY.toLowerCase()), false));
        rdbTableInfo.setDriverName(MathUtil.getString(props.get(RdbTableInfo.DRIVER_NAME)));
        rdbTableInfo.setFastCheck(MathUtil.getBoolean(props.getOrDefault(RdbTableInfo.FAST_CHECK.toLowerCase(), true)));

        rdbTableInfo.setType(CURR_TYPE);
        rdbTableInfo.setDatabaseName(MathUtil.getString(props.get("databasename")));
        rdbTableInfo.setLoadUrl(MathUtil.getString(props.get("loadurl")));
        rdbTableInfo.setJdbcUrl(MathUtil.getString(props.get("jdbcurl")));

        // sink的相关配置
        Map<String, String> propsMap = new HashMap<>();
        fieldMap(propsMap, "sink.semantic", props.get("sink.semantic"));
        fieldMap(propsMap, "sink.buffer-flush.max-bytes", props.get("sink.buffer-flush.max-bytes"));
        fieldMap(propsMap, "sink.buffer-flush.max-rows", props.get("sink.buffer-flush.max-rows"));
        fieldMap(propsMap, "sink.buffer-flush.interval-ms", props.get("sink.buffer-flush.interval-ms"));
        fieldMap(propsMap, "sink.max-retries", props.get("sink.max-retries"));
        fieldMap(propsMap, "sink.connect.timeout-ms", props.get("sink.connect.timeout-ms"));
        fieldMap(propsMap, "sink.properties.strip_outer_array", props.get("sink.properties.strip_outer_array"));
        fieldMap(propsMap, "sink.properties.format", props.get("sink.properties.format"));
        fieldMap(propsMap, "sink.properties.column_separator", props.get("sink.properties.column_separator"));
        fieldMap(propsMap, "sink.properties.row_delimiter", props.get("sink.properties.row_delimiter"));
        rdbTableInfo.setSinkProperties(propsMap);

        return rdbTableInfo;
    }

    public void fieldMap(Map<String, String> propsMap, String key, Object value) {
        String sinkSemantic = MathUtil.getString(value);
        if (StringUtils.isNotBlank(sinkSemantic)) {
            propsMap.put(key, sinkSemantic);
        }
    }
}
