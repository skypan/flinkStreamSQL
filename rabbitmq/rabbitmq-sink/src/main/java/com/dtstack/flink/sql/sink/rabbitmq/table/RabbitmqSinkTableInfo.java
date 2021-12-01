package com.dtstack.flink.sql.sink.rabbitmq.table;

import com.dtstack.flink.sql.table.AbstractTargetTableInfo;
import com.google.common.base.Preconditions;

public class RabbitmqSinkTableInfo extends AbstractTargetTableInfo {

    public static final String HOST_KEY = "host";

    public static final String PORT_KEY = "port";

    public static final String USERNAME_KEY = "username";

    public static final String PASSWORD_KEY = "password";

    public static final String QUEUE_KEY = "queue";

    public static final String TYPE_KEY = "type";

    private String name;

    private String host;

    private int port;

    private String username;

    private String password;

    private String queue;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public boolean check() {
        Preconditions.checkNotNull(host, "rabbitmq host can not be empty");
        Preconditions.checkNotNull(port, "rabbitmq port can not be empty");
        Preconditions.checkNotNull(queue, "rabbitmq queue can not be empty");
        return false;
    }
}
