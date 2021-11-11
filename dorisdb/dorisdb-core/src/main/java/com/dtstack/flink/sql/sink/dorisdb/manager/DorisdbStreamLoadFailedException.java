package com.dtstack.flink.sql.sink.dorisdb.manager;

import java.io.IOException;
import java.util.Map;

public class DorisdbStreamLoadFailedException extends IOException {
    static final long serialVersionUID = 1L;

    private final Map<String, Object> response;

    public DorisdbStreamLoadFailedException(String message, Map<String, Object> response) {
        super(message);
        this.response = response;
    }

    public Map<String, Object> getFailedResponse() {
        return response;
    }
}
