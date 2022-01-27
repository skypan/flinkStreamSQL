package com.dtstack.flink.sql.sink.dorisdb.builder;

import com.alibaba.fastjson.JSON;
import com.dtstack.flink.sql.sink.dorisdb.row.DorisdbSinkRowBuilder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;
import org.apache.flink.table.data.*;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.data.binary.BinaryMapData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CustomDorisSinkRowBuilder implements DorisdbSinkRowBuilder<Tuple2<Boolean, Row>> {

    private String[] columns;

    private DataType[] dataTypes;
    private final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");

    public CustomDorisSinkRowBuilder(String[] columns, DataType[] dataTypes) {
        this.columns = columns;
        this.dataTypes = dataTypes;
    }

    @Override
    public void accept(Object[] values, Tuple2<Boolean, Row> tuple2) {
        Row row = tuple2.f1;
        int nlen = values.length;
        for (int i = 0; i < nlen; i++) {
            values[i] = typeConvertion(row.getField(i), dataTypes[i].getLogicalType());
        }
    }

    private Object typeConvertion(Object objValue, LogicalType type) {
        if (objValue == null)
            return null;
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return (Boolean) objValue ? 1L : 0L;
            case TINYINT:
                return (Byte) objValue;
            case SMALLINT:
                return (Short) objValue;
            case INTEGER:
                return (Integer) objValue;
            case BIGINT:
                return (Long) objValue;
            case FLOAT:
                return (Float) objValue;
            case DOUBLE:
                return (Double) objValue;
            case CHAR:
            case VARCHAR:
                return objValue.toString();
            case DATE:
                return dateFormatter.format(Date.valueOf(LocalDate.ofEpochDay((Integer) objValue)));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampPrecision = ((TimestampType) type).getPrecision();
                return ((Timestamp) objValue).toLocalDateTime().toString();
            case DECIMAL: // for both largeint and decimal
//                final int decimalPrecision = ((DecimalType) type).getPrecision();
//                final int decimalScale = ((DecimalType) type).getScale();
                return (BigDecimal) objValue;
            case BINARY:
                final byte[] bts = (byte[]) objValue;
                long value = 0;
                for (int i = 0; i < bts.length; i++) {
                    value += (bts[bts.length - i - 1] & 0xffL) << (8 * i);
                }
                return value;
            case ARRAY:
                return convertNestedArray((ArrayData) objValue, type);
            case MAP:
                return convertNestedMap((MapData) objValue, type);
            case ROW:
                RowType rType = (RowType) type;
                Map<String, Object> m = Maps.newHashMap();
                RowData row = (RowData) objValue;
                rType.getFields().parallelStream().forEach(f -> m.put(f.getName(), typeConvertion(row, f.getType())));
                return m;
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    private List<Object> convertNestedArray(ArrayData arrData, LogicalType type) {
        if (arrData instanceof GenericArrayData) {
            return Lists.newArrayList(((GenericArrayData) arrData).toObjectArray());
        }
        if (arrData instanceof BinaryArrayData) {
            LogicalType lt = ((ArrayType) type).getElementType();
            List<Object> data = Lists.newArrayList(((BinaryArrayData) arrData).toObjectArray(lt));
            if (LogicalTypeRoot.ROW.equals(lt.getTypeRoot())) {
                RowType rType = (RowType) lt;
                // parse nested row data
                return data.parallelStream().map(row -> {
                    Map<String, Object> m = Maps.newHashMap();
                    rType.getFields().parallelStream().forEach(f -> m.put(f.getName(), typeConvertion((RowData) row, f.getType())));
                    return JSON.toJSONString(m);
                }).collect(Collectors.toList());
            }
            if (LogicalTypeRoot.MAP.equals(lt.getTypeRoot())) {
                // traversal of the nested map
                return data.parallelStream().map(m -> convertNestedMap((MapData) m, lt)).collect(Collectors.toList());
            }
            if (LogicalTypeRoot.DATE.equals(lt.getTypeRoot())) {
                return data.parallelStream().map(date -> dateFormatter.format(Date.valueOf(LocalDate.ofEpochDay((Integer) date)))).collect(Collectors.toList());
            }
            if (LogicalTypeRoot.ARRAY.equals(lt.getTypeRoot())) {
                // traversal of the nested array
                return data.parallelStream().map(arr -> convertNestedArray((ArrayData) arr, lt)).collect(Collectors.toList());
            }
            return data;
        }
        throw new UnsupportedOperationException(String.format("Unsupported array data: %s", arrData.getClass()));
    }

    private Map<Object, Object> convertNestedMap(MapData mapData, LogicalType type) {
        if (mapData instanceof GenericMapData) {
            HashMap<Object, Object> m = Maps.newHashMap();
            for (Object k : ((GenericArrayData) ((GenericMapData) mapData).keyArray()).toObjectArray()) {
                m.put(k, ((GenericMapData) mapData).get(k));
            }
            return m;
        }
        if (mapData instanceof BinaryMapData) {
            Map<Object, Object> result = Maps.newHashMap();
            LogicalType valType = ((MapType) type).getValueType();
            Map<?, ?> javaMap = ((BinaryMapData) mapData).toJavaMap(((MapType) type).getKeyType(), valType);
            for (Map.Entry<?, ?> en : javaMap.entrySet()) {
                if (LogicalTypeRoot.MAP.equals(valType.getTypeRoot())) {
                    // traversal of the nested map
                    result.put(en.getKey().toString(), convertNestedMap((MapData) en.getValue(), valType));
                    continue;
                }
                if (LogicalTypeRoot.DATE.equals(valType.getTypeRoot())) {
                    result.put(en.getKey().toString(), dateFormatter.format(Date.valueOf(LocalDate.ofEpochDay((Integer) en.getValue()))));
                    continue;
                }
                if (LogicalTypeRoot.ARRAY.equals(valType.getTypeRoot())) {
                    result.put(en.getKey().toString(), convertNestedArray((ArrayData) en.getValue(), valType));
                    continue;
                }
                result.put(en.getKey().toString(), en.getValue());
            }
            return result;
        }
        throw new UnsupportedOperationException(String.format("Unsupported map data: %s", mapData.getClass()));
    }
}
