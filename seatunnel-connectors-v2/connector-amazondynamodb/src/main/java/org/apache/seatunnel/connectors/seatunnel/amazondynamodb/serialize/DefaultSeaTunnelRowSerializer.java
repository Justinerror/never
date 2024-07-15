/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.amazondynamodb.serialize;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.common.utils.TimeUtils;
import org.apache.seatunnel.connectors.seatunnel.amazondynamodb.config.AmazonDynamoDBSourceOptions;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DefaultSeaTunnelRowSerializer implements SeaTunnelRowSerializer {

    private final SeaTunnelRowType seaTunnelRowType;
    private final AmazonDynamoDBSourceOptions amazondynamodbSourceOptions;
    private final List<AttributeValue.Type> measurementsType;

    private final DateTimeUtils.Formatter dateTimeFormatter =
            DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS_SSSSSS;
    private final DateUtils.Formatter dateFormatter = DateUtils.Formatter.YYYY_MM_DD;

    private final TimeUtils.Formatter timeFormatter = TimeUtils.Formatter.HH_MM_SS;

    public DefaultSeaTunnelRowSerializer(
            SeaTunnelRowType seaTunnelRowType,
            AmazonDynamoDBSourceOptions amazondynamodbSourceOptions) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.amazondynamodbSourceOptions = amazondynamodbSourceOptions;
        this.measurementsType = convertTypes(seaTunnelRowType);
    }

    @Override
    public PutItemRequest serialize(SeaTunnelRow seaTunnelRow) {
        HashMap<String, AttributeValue> itemValues = new HashMap<>();
        for (int index = 0; index < seaTunnelRowType.getFieldNames().length; index++) {
            String fieldName = seaTunnelRowType.getFieldName(index);
            itemValues.put(
                    fieldName,
                    convertItem(
                            fieldName,
                            seaTunnelRow.getField(index),
                            seaTunnelRowType.getFieldType(index),
                            measurementsType.get(index)));
        }
        return PutItemRequest.builder()
                .tableName(amazondynamodbSourceOptions.getTable())
                .item(itemValues)
                .build();
    }

    private List<AttributeValue.Type> convertTypes(SeaTunnelRowType seaTunnelRowType) {
        List<AttributeValue.Type> types = new ArrayList<>();
        for (int i = 0; i < seaTunnelRowType.getFieldTypes().length; i++) {
            types.add(
                    convertType(
                            seaTunnelRowType.getFieldName(i), seaTunnelRowType.getFieldType(i)));
        }
        return types;
    }

    private AttributeValue.Type convertType(String field, SeaTunnelDataType<?> seaTunnelDataType) {
        switch (seaTunnelDataType.getSqlType()) {
            case INT:
            case TINYINT:
            case SMALLINT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                return AttributeValue.Type.N;
            case STRING:
            case DATE:
            case TIME:
            case TIMESTAMP:
                return AttributeValue.Type.S;
            case BOOLEAN:
                return AttributeValue.Type.BOOL;
            case NULL:
                return AttributeValue.Type.NUL;
            case BYTES:
                return AttributeValue.Type.B;
            case MAP:
                return AttributeValue.Type.M;
            case ARRAY:
                return AttributeValue.Type.L;
            default:
                throw CommonError.convertToConnectorTypeError(
                        "AmazonDynamoDB", seaTunnelDataType.getSqlType().toString(), field);
        }
    }

    private AttributeValue convertItem(
            String field,
            Object value,
            SeaTunnelDataType seaTunnelDataType,
            AttributeValue.Type measurementsType) {
        if (value == null) {
            return AttributeValue.builder().nul(true).build();
        }
        switch (measurementsType) {
            case N:
                return AttributeValue.builder()
                        .n(Integer.toString(((Number) value).intValue()))
                        .build();
            case S:
                if (seaTunnelDataType.getSqlType().equals(SqlType.DATE)) {
                    return AttributeValue.builder()
                            .s(DateUtils.toString((LocalDate) value, dateFormatter))
                            .build();
                } else if (seaTunnelDataType.getSqlType().equals(SqlType.TIME)) {
                    return AttributeValue.builder()
                            .s(TimeUtils.toString((LocalTime) value, timeFormatter))
                            .build();
                } else if (seaTunnelDataType.getSqlType().equals(SqlType.TIMESTAMP)) {
                    return AttributeValue.builder()
                            .s(DateTimeUtils.toString((LocalDateTime) value, dateTimeFormatter))
                            .build();
                }
                return AttributeValue.builder().s(String.valueOf(value)).build();
            case BOOL:
                return AttributeValue.builder().bool((Boolean) value).build();
            case B:
                return AttributeValue.builder()
                        .b(SdkBytes.fromByteArrayUnsafe((byte[]) value))
                        .build();
            case SS:
                return AttributeValue.builder().ss((Collection<String>) value).build();
            case NS:
                return AttributeValue.builder()
                        .ns(
                                ((Collection<Number>) value)
                                        .stream()
                                                .map(Object::toString)
                                                .collect(Collectors.toList()))
                        .build();
            case BS:
                return AttributeValue.builder()
                        .bs(
                                ((Collection<Number>) value)
                                        .stream()
                                                .map(
                                                        number ->
                                                                SdkBytes.fromByteArray(
                                                                        (byte[]) value))
                                                .collect(Collectors.toList()))
                        .build();
            case M:
                MapType<?, ?> mapType = (MapType<?, ?>) seaTunnelDataType;
                Map<String, Object> map = (Map) value;
                Map<String, AttributeValue> resultMap = new HashMap<>(map.size());
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    String mapKeyName = entry.getKey();
                    resultMap.put(
                            mapKeyName,
                            convertItem(
                                    field,
                                    entry.getValue(),
                                    mapType.getValueType(),
                                    convertType(field, mapType.getValueType())));
                }
                return AttributeValue.builder().m(resultMap).build();
            case L:
                ArrayType<?, ?> arrayType = (ArrayType<?, ?>) seaTunnelDataType;
                SeaTunnelDataType<?> elementType = arrayType.getElementType();
                Object[] l = (Object[]) value;
                return AttributeValue.builder()
                        .l(
                                Stream.of(l)
                                        .map(
                                                o ->
                                                        convertItem(
                                                                field,
                                                                o,
                                                                elementType,
                                                                convertType(field, elementType)))
                                        .collect(Collectors.toList()))
                        .build();
            case NUL:
                return AttributeValue.builder().nul(true).build();
            default:
                throw CommonError.convertToConnectorTypeError(
                        "AmazonDynamoDB", measurementsType.toString(), field);
        }
    }
}
