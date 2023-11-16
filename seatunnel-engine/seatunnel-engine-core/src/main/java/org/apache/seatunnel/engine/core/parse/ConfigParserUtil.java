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

package org.apache.seatunnel.engine.core.parse;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.engine.common.exception.JobDefineCheckException;

import org.apache.commons.collections4.CollectionUtils;

import lombok.extern.slf4j.Slf4j;
import scala.Tuple2;

import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.seatunnel.api.common.CommonOptions.PLUGIN_NAME;
import static org.apache.seatunnel.api.common.CommonOptions.RESULT_TABLE_NAME;
import static org.apache.seatunnel.api.common.CommonOptions.SOURCE_TABLE_NAME;
import static org.apache.seatunnel.api.table.factory.FactoryUtil.DEFAULT_ID;

@Slf4j
public final class ConfigParserUtil {
    private ConfigParserUtil() {}

    public static <T extends Factory> Set<URL> getFactoryUrls(
            ReadonlyConfig readonlyConfig,
            ClassLoader classLoader,
            Class<T> factoryClass,
            String factoryId) {
        Set<URL> factoryUrls = new HashSet<>();
        URL factoryUrl =
                FactoryUtil.getFactoryUrl(
                        FactoryUtil.discoverFactory(classLoader, factoryClass, factoryId));
        factoryUrls.add(factoryUrl);
        return factoryUrls;
    }

    public static void checkGraph(
            List<? extends Config> sources,
            List<? extends Config> transforms,
            List<? extends Config> sinks) {
        log.debug("Check whether this config file can generate DAG");
        if (CollectionUtils.isEmpty(sources) || CollectionUtils.isEmpty(sinks)) {
            throw new JobDefineCheckException("Source And Sink can not be null");
        }

        log.debug("Start checking the correctness of the DAG.");
        log.debug(
                String.format(
                        "Phase 1: Check whether '%s' option is configured.",
                        RESULT_TABLE_NAME.key()));
        checkExistTableId(sources);
        checkExistTableId(transforms);
        log.debug(
                String.format(
                        "Phase 2: Check whether '%s' option is configured.",
                        SOURCE_TABLE_NAME.key()));
        checkExistInputTableId(transforms);
        checkExistInputTableId(sinks);

        log.debug("Phase 3: Generate virtual vertices.");
        Map<String, Tuple2<Config, VertexStatus>> vertexStatusMap = new HashMap<>();
        fillVirtualVertices(sources, vertexStatusMap);
        fillVirtualVertices(transforms, vertexStatusMap);

        log.debug("Phase 4: Check if a non-existent vertex is used.");
        checkInputId(transforms, vertexStatusMap);
        checkInputId(sinks, vertexStatusMap);

        log.debug("Phase 5: Check if there are unused vertex.");
        checkLinked(vertexStatusMap);

        log.debug("The DAG is correct.");
    }

    private static void fillVirtualVertices(
            List<? extends Config> configs,
            Map<String, Tuple2<Config, VertexStatus>> vertexStatusMap) {
        for (Config config : configs) {
            vertexStatusMap.compute(
                    config.getString(RESULT_TABLE_NAME.key()),
                    (id, old) -> {
                        if (old != null) {
                            throw new JobDefineCheckException(
                                    String.format(
                                            "The value of the '%s' option of the (%s and %s) plugins is both '%s', and they must be different.",
                                            RESULT_TABLE_NAME.key(),
                                            config.getString(PLUGIN_NAME.key()),
                                            old._1().getString(PLUGIN_NAME.key()),
                                            id));
                        }
                        return new Tuple2<>(config, VertexStatus.CREATED);
                    });
        }
    }

    private static void checkInputId(
            List<? extends Config> configs,
            Map<String, Tuple2<Config, VertexStatus>> vertexStatusMap) {
        for (Config config : configs) {
            List<String> inputIds = getInputIds(ReadonlyConfig.fromConfig(config));
            inputIds.forEach(
                    inputId ->
                            vertexStatusMap.compute(
                                    inputId,
                                    (id, old) -> {
                                        if (old == null) {
                                            throw new JobDefineCheckException(
                                                    String.format(
                                                            "The '%s' option configured in [%s] is incorrect, and the source/transform[%s] is not found.",
                                                            SOURCE_TABLE_NAME.key(),
                                                            config.getString(PLUGIN_NAME.key()),
                                                            id));
                                        }
                                        return new Tuple2<>(old._1(), VertexStatus.LINKED);
                                    }));
        }
    }

    private static void checkLinked(Map<String, Tuple2<Config, VertexStatus>> vertexStatusMap) {
        vertexStatusMap.forEach(
                (id, vertex) -> {
                    if (vertex._2() == VertexStatus.CREATED) {
                        throw new JobDefineCheckException(
                                String.format(
                                        "The '%s' option configured is incorrect, this table(%s) belonging to source/transform(%s) is not used.",
                                        SOURCE_TABLE_NAME.key(),
                                        id,
                                        vertex._1().getString(PLUGIN_NAME.key())));
                    }
                });
    }

    private static void checkExistTableId(List<? extends Config> configs) {
        for (Config config : configs) {
            if (!config.hasPath(RESULT_TABLE_NAME.key())) {
                throw new JobDefineCheckException(
                        String.format(
                                "The source/transform(%s) is not configured with '%s' option",
                                config.getString(PLUGIN_NAME.key()), RESULT_TABLE_NAME.key()),
                        new OptionValidationException(RESULT_TABLE_NAME));
            }
        }
    }

    private static void checkExistInputTableId(List<? extends Config> configs) {
        for (Config config : configs) {
            if (!config.hasPath(SOURCE_TABLE_NAME.key())) {
                throw new JobDefineCheckException(
                        String.format(
                                "The transform/sink(%s) is not configured with '%s' option",
                                config.getString(PLUGIN_NAME.key()), SOURCE_TABLE_NAME.key()),
                        new OptionValidationException(SOURCE_TABLE_NAME));
            }
        }
    }

    private static String getTableId(ReadonlyConfig config) {
        return config.getOptional(RESULT_TABLE_NAME).orElse(DEFAULT_ID);
    }

    static List<String> getInputIds(ReadonlyConfig config) {
        return config.getOptional(SOURCE_TABLE_NAME).orElse(Collections.singletonList(DEFAULT_ID));
    }

    public static String getFactoryId(ReadonlyConfig readonlyConfig) {
        return readonlyConfig.get(PLUGIN_NAME);
    }

    public static String getFactoryId(Config config) {
        return getFactoryId(ReadonlyConfig.fromConfig(config));
    }

    private enum VertexStatus {
        CREATED,
        LINKED
    }
}
