package com.dtstack.flink.sql.sink.dorisdb.connection;

import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Optional;

public class DorisdbJdbcConnectionOptions  implements Serializable {

    protected final String url;
    protected final String driverName;
    protected final String cjDriverName;
    @Nullable
    protected final String username;
    @Nullable
    protected final String password;

    public DorisdbJdbcConnectionOptions(String url, String username, String password) {
        this.url = Preconditions.checkNotNull(url, "jdbc url is empty");
        this.driverName = "com.mysql.jdbc.Driver";
        this.cjDriverName = "com.mysql.cj.jdbc.Driver";
        this.username = username;
        this.password = password;
    }

    public String getDbURL() {
        return url;
    }

    public String getDriverName() {
        return driverName;
    }

    public String getCjDriverName() {
        return cjDriverName;
    }

    public Optional<String> getUsername() {
        return Optional.ofNullable(username);
    }

    public Optional<String> getPassword() {
        return Optional.ofNullable(password);
    }
}
