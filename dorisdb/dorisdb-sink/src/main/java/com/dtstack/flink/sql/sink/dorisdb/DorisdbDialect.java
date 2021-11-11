package com.dtstack.flink.sql.sink.dorisdb;

import com.dtstack.flink.sql.sink.rdb.dialect.JDBCDialect;

import java.util.Optional;

/**
 * DorisDB Dialect
 */
public class DorisdbDialect implements JDBCDialect {

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:mysql:");
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("com.mysql.jdbc.Driver");
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }
}
