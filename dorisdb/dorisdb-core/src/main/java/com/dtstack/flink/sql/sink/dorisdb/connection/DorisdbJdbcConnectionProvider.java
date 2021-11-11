package com.dtstack.flink.sql.sink.dorisdb.connection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DorisdbJdbcConnectionProvider implements DorisdbJdbcConnectionIProvider, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(DorisdbJdbcConnectionProvider.class);

    private final DorisdbJdbcConnectionOptions jdbcOptions;

    private transient volatile Connection connection;

    public DorisdbJdbcConnectionProvider(DorisdbJdbcConnectionOptions jdbcOptions) {
        this.jdbcOptions = jdbcOptions;
    }

    @Override
    public Connection getConnection() throws SQLException, ClassNotFoundException {
        if (connection == null) {
            synchronized (this) {
                if (connection == null) {
                    try {
                        Class.forName(jdbcOptions.getCjDriverName());
                    } catch (ClassNotFoundException ex) {
                        Class.forName(jdbcOptions.getDriverName());
                    }
                    if (jdbcOptions.getUsername().isPresent()) {
                        connection = DriverManager.getConnection(jdbcOptions.getDbURL(), jdbcOptions.getUsername().orElse(null), jdbcOptions.getPassword().orElse(null));
                    } else {
                        connection = DriverManager.getConnection(jdbcOptions.getDbURL());
                    }
                }
            }
        }
        return connection;
    }

    @Override
    public Connection reestablishConnection() throws SQLException, ClassNotFoundException {
        try {
            connection.close();
        } catch (SQLException e) {
            LOG.info("JDBC connection close failed.", e);
        } finally {
            connection = null;
        }
        connection = getConnection();
        return connection;
    }
}
