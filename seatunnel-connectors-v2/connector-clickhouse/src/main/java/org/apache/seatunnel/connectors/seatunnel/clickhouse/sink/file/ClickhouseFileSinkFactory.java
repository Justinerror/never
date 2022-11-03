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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.file;

import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.Config.CLICKHOUSE_LOCAL_PATH;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.Config.COPY_METHOD;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.Config.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.Config.FIELDS;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.Config.HOST;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.Config.NODE_ADDRESS;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.Config.NODE_PASS;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.Config.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.Config.SHARDING_KEY;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.Config.TABLE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.Config.USERNAME;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;

public class ClickhouseFileSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "ClickhouseFileSink";
    }

    @Override
    public OptionRule optionRule() {
        // TODO fix option rule not match with clickhouse file config key, because config use object and array type for node pass.
        return OptionRule.builder().required(HOST, TABLE, DATABASE, USERNAME, PASSWORD, CLICKHOUSE_LOCAL_PATH)
            .optional(COPY_METHOD, SHARDING_KEY, FIELDS, NODE_PASS, NODE_ADDRESS).build();
    }
}
