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

package org.apache.seatunnel.config;

import org.apache.seatunnel.apis.BaseSink;
import org.apache.seatunnel.apis.BaseSource;
import org.apache.seatunnel.apis.BaseTransform;
import org.apache.seatunnel.common.config.ConfigRuntimeException;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.env.Execution;
import org.apache.seatunnel.env.RuntimeEnv;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.batch.FlinkBatchExecution;
import org.apache.seatunnel.flink.stream.FlinkStreamExecution;
import org.apache.seatunnel.plugin.Plugin;
import org.apache.seatunnel.spark.SparkEnvironment;
import org.apache.seatunnel.spark.batch.SparkBatchExecution;
import org.apache.seatunnel.spark.stream.SparkStreamingExecution;
import org.apache.seatunnel.spark.structuredstream.StructuredStreamingExecution;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigResolveOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

public class ConfigBuilder<ENVIRONMENT extends RuntimeEnv> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigBuilder.class);

    private static final String PLUGIN_NAME_KEY = "plugin_name";
    private final String configFile;
    private final EngineType engine;
    private final Config config;
    private JobMode jobMode;
    private Config envConfig;
    private final ENVIRONMENT env;
    private final PluginFactory<ENVIRONMENT> pluginFactory;

    public ConfigBuilder(String configFile, EngineType engine) {
        this.configFile = configFile;
        this.engine = engine;
        this.pluginFactory = new PluginFactory<>(engine);
        this.config = load();
        this.env = createEnv();
    }

    private Config load() {

        if (configFile.isEmpty()) {
            throw new ConfigRuntimeException("Please specify config file");
        }

        LOGGER.info("Loading config file: {}", configFile);

        // variables substitution / variables resolution order:
        // config file --> system environment --> java properties
        Config config = ConfigFactory
                .parseFile(new File(configFile))
                .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                .resolveWith(ConfigFactory.systemProperties(),
                        ConfigResolveOptions.defaults().setAllowUnresolved(true));

        ConfigRenderOptions options = ConfigRenderOptions.concise().setFormatted(true);
        LOGGER.info("parsed config file: {}", config.root().render(options));
        return config;
    }

    public Config getEnvConfigs() {
        return envConfig;
    }

    public ENVIRONMENT getEnv() {
        return env;
    }

    private void setJobMode(Config envConfig) {
        if (envConfig.hasPath("job.mode")) {
            jobMode = envConfig.getEnum(JobMode.class, "job.mode");
        } else {
            //Compatible with previous logic
            List<? extends Config> sourceConfigList = config.getConfigList(PluginType.SOURCE.getType());
            jobMode = sourceConfigList.get(0).getString(PLUGIN_NAME_KEY).toLowerCase().endsWith("stream") ? JobMode.STREAMING : JobMode.BATCH;
        }

    }

    private boolean checkIsContainHive() {
        List<? extends Config> sourceConfigList = config.getConfigList(PluginType.SOURCE.getType());
        for (Config c : sourceConfigList) {
            if (c.getString(PLUGIN_NAME_KEY).toLowerCase().contains("hive")) {
                return true;
            }
        }
        List<? extends Config> sinkConfigList = config.getConfigList(PluginType.SINK.getType());
        for (Config c : sinkConfigList) {
            if (c.getString(PLUGIN_NAME_KEY).toLowerCase().contains("hive")) {
                return true;
            }
        }
        return false;
    }

    /**
     * check if config is valid.
     **/
    public void checkConfig() {
        this.createEnv();
        this.pluginFactory.createPlugins(config, PluginType.SOURCE);
        this.pluginFactory.createPlugins(config, PluginType.TRANSFORM);
        this.pluginFactory.createPlugins(config, PluginType.SINK);
    }

    public <T extends Plugin<ENVIRONMENT>> List<T> createPlugins(PluginType type) {
        return pluginFactory.createPlugins(config, type);
    }

    private ENVIRONMENT createEnv() {
        envConfig = config.getConfig("env");
        boolean enableHive = checkIsContainHive();
        ENVIRONMENT env;
        switch (engine) {
            case SPARK:
                env = (ENVIRONMENT) new SparkEnvironment().setEnableHive(enableHive);
                break;
            case FLINK:
                env = (ENVIRONMENT) new FlinkEnvironment();
                break;
            default:
                throw new IllegalArgumentException("Engine: " + engine + " is not supported");
        }
        setJobMode(envConfig);
        env.setConfig(envConfig).setJobMode(jobMode).prepare();
        return env;
    }

    public Execution<BaseSource<ENVIRONMENT>, BaseTransform<ENVIRONMENT>, BaseSink<ENVIRONMENT>, ENVIRONMENT> createExecution() {
        Execution execution = null;
        switch (engine) {
            case SPARK:
                SparkEnvironment sparkEnvironment = (SparkEnvironment) env;
                if (JobMode.STREAMING.equals(jobMode)) {
                    execution = new SparkStreamingExecution(sparkEnvironment);
                } else if (JobMode.STRUCTURED_STREAMING.equals(jobMode)) {
                    execution = new StructuredStreamingExecution(sparkEnvironment);
                } else {
                    execution = new SparkBatchExecution(sparkEnvironment);
                }
                break;
            case FLINK:
                FlinkEnvironment flinkEnvironment = (FlinkEnvironment) env;
                if (JobMode.STREAMING.equals(jobMode)) {
                    execution = new FlinkStreamExecution(flinkEnvironment);
                } else {
                    execution = new FlinkBatchExecution(flinkEnvironment);
                }
                break;
            default:
                throw new IllegalArgumentException("No suitable engine");
        }
        LOGGER.info("current execution is [{}]", execution.getClass().getName());
        return (Execution<BaseSource<ENVIRONMENT>, BaseTransform<ENVIRONMENT>, BaseSink<ENVIRONMENT>, ENVIRONMENT>) execution;
    }

}
