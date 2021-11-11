package com.dtstack.flink.sql.sink.dorisdb.table;

import com.dtstack.flink.sql.sink.rdb.table.RdbTableInfo;

import java.util.Map;

public class DorisdbTableInfo extends RdbTableInfo {

    private String loadUrl;

    private String databaseName;

    private String jdbcUrl;

    private Map<String, String> sinkProperties;

    public String getLoadUrl() {
        return loadUrl;
    }

    public void setLoadUrl(String loadUrl) {
        this.loadUrl = loadUrl;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public Map<String, String> getSinkProperties() {
        return sinkProperties;
    }

    public void setSinkProperties(Map<String, String> sinkProperties) {
        this.sinkProperties = sinkProperties;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }
}
