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

package org.apache.seatunnel.transform.common;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TransformCommonOptions {

    public static final Option<List<Map<String, Object>>> MULTI_TABLES =
            Options.key("table_transform")
                    .type(new TypeReference<List<Map<String, Object>>>() {})
                    .defaultValue(Collections.emptyList())
                    .withDescription("The table transform config");

    public static final Option<String> TABLE_PATH =
            Options.key("table_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The table path of catalog table");

    public static final Option<String> TABLE_MATCH_REGEX =
            Options.key("table_match_regex")
                    .stringType()
                    .defaultValue(".*")
                    .withDescription("The regex to match the table path");
}
