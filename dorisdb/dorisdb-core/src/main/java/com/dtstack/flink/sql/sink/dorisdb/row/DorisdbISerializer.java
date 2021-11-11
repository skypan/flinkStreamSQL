package com.dtstack.flink.sql.sink.dorisdb.row;

import java.io.Serializable;

public interface DorisdbISerializer extends Serializable {
    String serialize(Object[] values);
}
