/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.inject;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.regex.Pattern;

public class DateTimeInjectFunction implements ClickhouseFieldInjectFunction {
    
    private static final Pattern PATTERN = Pattern.compile("(DateTime.*)");
    
    @Override
    public void injectFields(PreparedStatement statement, int index, Object value) throws SQLException {
        if (value instanceof Timestamp) {
            statement.setTimestamp(index, (Timestamp) value);
        } else if (value instanceof LocalDateTime) {
            statement.setObject(index, value);
        } else {
            statement.setTimestamp(index, Timestamp.valueOf(value.toString()));
        }
    }
    
    @Override
    public boolean isCurrentFieldType(String fieldType) {
        return PATTERN.matcher(fieldType).matches();
    }
}
