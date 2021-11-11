package com.dtstack.flink.sql.sink.dorisdb.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.HashSet;
import java.util.Set;

public class DorisdbDynamicTableSinkFactory implements DynamicTableSinkFactory {
    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validateExcept(DorisdbSinkOptions.SINK_PROPERTIES_PREFIX);
        ReadableConfig options = helper.getOptions();
        // validate some special properties
        DorisdbSinkOptions sinkOptions = new DorisdbSinkOptions(options, context.getCatalogTable().getOptions());
        TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        return new DorisdbDynamicTableSink(sinkOptions, physicalSchema);
    }

    @Override
    public String factoryIdentifier() {
        return "Dorisdb";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(DorisdbSinkOptions.JDBC_URL);
        requiredOptions.add(DorisdbSinkOptions.LOAD_URL);
        requiredOptions.add(DorisdbSinkOptions.DATABASE_NAME);
        requiredOptions.add(DorisdbSinkOptions.TABLE_NAME);
        requiredOptions.add(DorisdbSinkOptions.USERNAME);
        requiredOptions.add(DorisdbSinkOptions.PASSWORD);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(DorisdbSinkOptions.SINK_BATCH_MAX_SIZE);
        optionalOptions.add(DorisdbSinkOptions.SINK_BATCH_MAX_ROWS);
        optionalOptions.add(DorisdbSinkOptions.SINK_BATCH_FLUSH_INTERVAL);
        optionalOptions.add(DorisdbSinkOptions.SINK_MAX_RETRIES);
        optionalOptions.add(DorisdbSinkOptions.SINK_SEMANTIC);
        return optionalOptions;
    }
}
