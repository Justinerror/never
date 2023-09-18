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

package org.apache.seatunnel.format.text;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class TextFormatHiveSchemaTest {
    public String content =
            String.join("\u0002", Arrays.asList("1", "2", "3", "4", "5", "6"))
                    + '\001'
                    + "tyrantlucifer"
                    + '\003'
                    + "18"
                    + '\002'
                    + "Kris"
                    + '\003'
                    + "21"
                    + '\002'
                    + "nullValueKey"
                    + '\003'
                    + '\002'
                    + '\003'
                    + "1231"
                    + "\001"
                    + "tyrantlucifer\001"
                    + "true\001"
                    + "1\001"
                    + "2\001"
                    + "3\001"
                    + "4\001"
                    + "6.66\001"
                    + "7.77\001"
                    + "8.8888888\001"
                    + '\001'
                    + "tyrantlucifer\001"
                    + "2022-09-24\001"
                    + "22:45:00\001"
                    + "2022-09-24 22:45:00\001"
                    + "20\002"
                    + "123@qq.com";

    public SeaTunnelRowType seaTunnelRowType;

    @BeforeEach
    public void initSeaTunnelRowType() {
        seaTunnelRowType =
                new SeaTunnelRowType(
                        new String[] {
                            "array_field",
                            "map_field",
                            "string_field",
                            "boolean_field",
                            "tinyint_field",
                            "smallint_field",
                            "int_field",
                            "bigint_field",
                            "float_field",
                            "double_field",
                            "decimal_field",
                            "null_field",
                            "bytes_field",
                            "date_field",
                            "time_field",
                            "timestamp_field",
                            "row_field"
                        },
                        new SeaTunnelDataType<?>[] {
                            ArrayType.INT_ARRAY_TYPE,
                            new MapType<>(BasicType.STRING_TYPE, BasicType.INT_TYPE),
                            BasicType.STRING_TYPE,
                            BasicType.BOOLEAN_TYPE,
                            BasicType.BYTE_TYPE,
                            BasicType.SHORT_TYPE,
                            BasicType.INT_TYPE,
                            BasicType.LONG_TYPE,
                            BasicType.FLOAT_TYPE,
                            BasicType.DOUBLE_TYPE,
                            new DecimalType(30, 8),
                            BasicType.VOID_TYPE,
                            PrimitiveByteArrayType.INSTANCE,
                            LocalTimeType.LOCAL_DATE_TYPE,
                            LocalTimeType.LOCAL_TIME_TYPE,
                            LocalTimeType.LOCAL_DATE_TIME_TYPE,
                            new SeaTunnelRowType(
                                    new String[] {
                                        "age", "email",
                                    },
                                    new SeaTunnelDataType<?>[] {
                                        BasicType.INT_TYPE, BasicType.STRING_TYPE,
                                    })
                        });
    }

    @Test
    public void testParse() throws IOException {
        TextDeserializationSchema deserializationSchema =
                TextDeserializationSchema.builder()
                        .seaTunnelRowType(seaTunnelRowType)
                        .delimiter("\u0001")
                        .build();
        TextSerializationHiveSchema serializationSchema =
                TextSerializationHiveSchema.builder()
                        .seaTunnelRowType(seaTunnelRowType)
                        .delimiter("#")
                        .collectionDelimiter(",")
                        .mapKeysDelimiter(":")
                        .build();
        SeaTunnelRow seaTunnelRow = deserializationSchema.deserialize(content.getBytes());
        String data = new String(serializationSchema.serialize(seaTunnelRow));
        Assertions.assertEquals(((Map<?, ?>) (seaTunnelRow.getField(1))).get("tyrantlucifer"), 18);
        Assertions.assertEquals(((Map<?, ?>) (seaTunnelRow.getField(1))).get("Kris"), 21);
        Assertions.assertArrayEquals(
                (byte[]) seaTunnelRow.getField(12), "tyrantlucifer".getBytes());
        Assertions.assertEquals(seaTunnelRow.getField(2), "tyrantlucifer");

        String[] result = data.split("#");
        Assertions.assertEquals(result[0], "1,2,3,4,5,6");
        Assertions.assertEquals(result[1], "tyrantlucifer:18,Kris:21,nullValueKey:null,null:1231");
        Assertions.assertEquals(result[16], "20,123@qq.com");
    }
}
