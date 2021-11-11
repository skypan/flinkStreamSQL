package com.dtstack.flink.sql.sink.dorisdb.row;

import java.io.Serializable;
import java.util.function.BiConsumer;

public interface DorisdbSinkRowBuilder<T> extends BiConsumer<Object[], T>, Serializable {
}
