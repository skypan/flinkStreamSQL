package com.dtstack.flink.sql.sink.dorisdb.connection;

import org.apache.flink.annotation.Internal;

import java.sql.Connection;

@Internal
public interface DorisdbJdbcConnectionIProvider {
    Connection getConnection() throws Exception;

    Connection reestablishConnection() throws Exception;
}
