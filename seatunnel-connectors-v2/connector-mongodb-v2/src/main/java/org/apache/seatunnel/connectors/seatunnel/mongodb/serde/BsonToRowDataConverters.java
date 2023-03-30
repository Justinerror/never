package org.apache.seatunnel.connectors.seatunnel.mongodb.serde;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.seatunnel.mongodb.exception.MongodbConnectorException;

import org.bson.Document;
import org.bson.types.Decimal128;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static org.apache.seatunnel.common.exception.CommonErrorCode.UNSUPPORTED_OPERATION;

public class BsonToRowDataConverters implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final DateTimeFormatter TIME_FORMAT =
            new DateTimeFormatterBuilder()
                    .appendPattern("HH:mm:ss")
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                    .toFormatter();

    public BsonToRowDataConverters() {}

    public BsonToRowDataConverter createConverter(SeaTunnelDataType<?> type) {
        return this.wrapIntoNullableConverter(this.createNotNullConverter(type));
    }

    private BsonToRowDataConverter createNotNullConverter(SeaTunnelDataType<?> type) {
        switch (type.getSqlType()) {
            case NULL:
                return (reuse, value) -> null;
            case BOOLEAN:
            case DOUBLE:
            case INT:
            case BIGINT:
                return (reuse, value) -> value;
            case BYTES:
                return (reuse, value) -> value.toString().getBytes();
            case TINYINT:
                return (reuse, value) -> ((Integer) value).byteValue();
            case SMALLINT:
                return (reuse, value) -> ((Integer) value).shortValue();
            case FLOAT:
                return (reuse, value) -> ((Double) value).floatValue();
            case STRING:
                return (reuse, value) -> value.toString();
            case DATE:
                return this.convertToLocalDate();
            case TIME:
                return this.convertToLocalTime();
            case TIMESTAMP:
                return this.convertToLocalDateTime();
            case DECIMAL:
                return (reuse, value) -> {
                    final int precision = ((DecimalType) type).getPrecision();
                    final int scale = ((DecimalType) type).getScale();
                    return fromBigDecimal(((Decimal128) value).bigDecimalValue(), precision, scale);
                };
            case ARRAY:
                return createArrayConverter((ArrayType<?, ?>) type);
            case MAP:
                return createMapConverter((MapType<?, ?>) type);
            case ROW:
                return this.createRowConverter((SeaTunnelRowType) type);
            default:
                throw new UnsupportedOperationException("Not support to parse type: " + type);
        }
    }

    private BsonToRowDataConverter convertToLocalDate() {
        return (reuse, value) ->
                ISO_LOCAL_DATE.parse(value.toString()).query(TemporalQueries.localDate());
    }

    private BsonToRowDataConverter convertToLocalTime() {
        return (reuse, value) -> {
            TemporalAccessor parsedTime = TIME_FORMAT.parse(value.toString());
            return parsedTime.query(TemporalQueries.localTime());
        };
    }

    private BsonToRowDataConverter convertToLocalDateTime() {
        return (reuse, value) -> {
            TemporalAccessor parsedTimestamp =
                    DateTimeFormatter.ISO_LOCAL_DATE_TIME.parse(value.toString());
            LocalTime localTime = parsedTimestamp.query(TemporalQueries.localTime());
            LocalDate localDate = parsedTimestamp.query(TemporalQueries.localDate());
            return LocalDateTime.of(localDate, localTime);
        };
    }

    private BsonToRowDataConverter createRowConverter(SeaTunnelRowType type) {
        SeaTunnelDataType<?>[] fieldTypes = type.getFieldTypes();
        BsonToRowDataConverter[] fieldConverters =
                Arrays.stream(fieldTypes)
                        .map(this::createConverter)
                        .toArray(BsonToRowDataConverter[]::new);
        int fieldCount = type.getTotalFields();

        return (reuse, value) -> {
            SeaTunnelRow containerRow;
            if (reuse != null) {
                containerRow = (SeaTunnelRow) reuse;
            } else {
                containerRow = new SeaTunnelRow(fieldCount);
            }

            Document document = (Document) value;
            Object[] fieldValue = document.values().toArray();
            for (int i = 0; i < fieldCount; ++i) {
                Object o = fieldValue[i];
                containerRow.setField(i, fieldConverters[i].convert(null, o));
            }
            return containerRow;
        };
    }

    private BsonToRowDataConverter createArrayConverter(ArrayType<?, ?> type) {
        BsonToRowDataConverter valueConverter = createConverter(type.getElementType());
        return (reuse, value) -> {
            ArrayList<?> v = (ArrayList<?>) value;
            Object arr = Array.newInstance(type.getElementType().getTypeClass(), v.size());
            for (int i = 0; i < v.size(); i++) {
                Array.set(arr, i, valueConverter.convert(null, v.get(i)));
            }
            return arr;
        };
    }

    private BsonToRowDataConverter createMapConverter(MapType<?, ?> type) {
        if (!type.getKeyType().getSqlType().equals(SqlType.STRING)) {
            throw new MongodbConnectorException(
                    UNSUPPORTED_OPERATION,
                    "Bson format doesn't support non-string as key type of map. The type is: "
                            + type.getKeyType().getSqlType().toString());
        }
        BsonToRowDataConverter valueConverter = createConverter(type.getValueType());
        return (reuse, value) -> {
            Map<String, Object> map = new HashMap<>();
            Document document = (Document) value;
            for (String key : document.keySet()) {
                map.put(key, valueConverter.convert(null, document.get(key)));
            }
            return map;
        };
    }

    private BsonToRowDataConverter wrapIntoNullableConverter(BsonToRowDataConverter converter) {
        return (reuse, object) -> object == null ? null : converter.convert(reuse, object);
    }

    public interface BsonToRowDataConverter extends Serializable {
        Object convert(Object reusedContainer, Object value);
    }

    private BigDecimal fromBigDecimal(BigDecimal bd, int precision, int scale) {
        bd = bd.setScale(scale, RoundingMode.HALF_UP);
        if (bd.precision() > precision) {
            return null;
        }
        return bd;
    }
}
