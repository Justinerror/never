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

package org.apache.seatunnel.connectors.seatunnel.kafka.source;

import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaConfig.BOOTSTRAP_SERVERS;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaConfig.COMMIT_ON_CHECKPOINT;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaConfig.CONSUMER_GROUP;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaConfig.PATTERN;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaConfig.TOPIC;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaConfig.SCHEMA;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaConfig.FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaConfig.DEFAULT_FORMAT;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.config.TypesafeConfigUtils;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.common.schema.SeaTunnelSchema;
import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaSourceState;

import org.apache.seatunnel.format.json.JsonDeserializationSchema;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;

import java.util.Properties;

@AutoService(SeaTunnelSource.class)
public class KafkaSource implements SeaTunnelSource<SeaTunnelRow, KafkaSourceSplit, KafkaSourceState> {

    private static final String DEFAULT_CONSUMER_GROUP = "SeaTunnel-Consumer-Group";

    private final ConsumerMetadata metadata = new ConsumerMetadata();
    private SeaTunnelRowType typeInfo;
    private JobContext jobContext;
    private DeserializationSchema<SeaTunnelRow> deserializationSchema;

    @Override
    public Boundedness getBoundedness() {
        return JobMode.BATCH.equals(jobContext.getJobMode()) ? Boundedness.BOUNDED : Boundedness.UNBOUNDED;
    }

    @Override
    public String getPluginName() {
        return "Kafka";
    }

    @Override
    public void prepare(Config config) throws PrepareFailException {
        CheckResult result = CheckConfigUtil.checkAllExists(config, TOPIC, BOOTSTRAP_SERVERS);
        if (!result.isSuccess()) {
            throw new PrepareFailException(getPluginName(), PluginType.SOURCE, result.getMsg());
        }
        this.metadata.setTopic(config.getString(TOPIC));
        if (config.hasPath(PATTERN)) {
            this.metadata.setPattern(config.getBoolean(PATTERN));
        }
        this.metadata.setBootstrapServers(config.getString(BOOTSTRAP_SERVERS));
        this.metadata.setProperties(new Properties());

        if (config.hasPath(CONSUMER_GROUP)) {
            this.metadata.setConsumerGroup(config.getString(CONSUMER_GROUP));
        } else {
            this.metadata.setConsumerGroup(DEFAULT_CONSUMER_GROUP);
        }

        if (config.hasPath(COMMIT_ON_CHECKPOINT)) {
            this.metadata.setCommitOnCheckpoint(config.getBoolean(COMMIT_ON_CHECKPOINT));
        }

        TypesafeConfigUtils.extractSubConfig(config, "kafka.", false).entrySet().forEach(e -> {
            this.metadata.getProperties().put(e.getKey(), String.valueOf(e.getValue().unwrapped()));
        });

        setDeserialization(config);
    }

    @Override
    public SeaTunnelRowType getProducedType() {
        return this.typeInfo;
    }

    @Override
    public SourceReader<SeaTunnelRow, KafkaSourceSplit> createReader(SourceReader.Context readerContext) throws Exception {
        return new KafkaSourceReader(this.metadata, this.typeInfo, deserializationSchema, readerContext);
    }

    @Override
    public SourceSplitEnumerator<KafkaSourceSplit, KafkaSourceState> createEnumerator(SourceSplitEnumerator.Context<KafkaSourceSplit> enumeratorContext) throws Exception {
        return new KafkaSourceSplitEnumerator(this.metadata, enumeratorContext);
    }

    @Override
    public SourceSplitEnumerator<KafkaSourceSplit, KafkaSourceState> restoreEnumerator(SourceSplitEnumerator.Context<KafkaSourceSplit> enumeratorContext, KafkaSourceState checkpointState) throws Exception {
        return new KafkaSourceSplitEnumerator(this.metadata, enumeratorContext, checkpointState);
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    private void setDeserialization(Config config) {
        if (config.hasPath(SCHEMA)) {
            Config schema = config.getConfig(SCHEMA);
            typeInfo = SeaTunnelSchema.buildWithConfig(schema).getSeaTunnelRowType();
        } else {
            typeInfo = SeaTunnelSchema.buildSimpleTextSchema();
        }
        // TODO: use format SPI
        // default use json format
        String format;
        if(config.hasPath(FORMAT)){
            if(DEFAULT_FORMAT.equals(config.getString(FORMAT))){
                deserializationSchema = new JsonDeserializationSchema(false, false, typeInfo);
            }else{
                format = config.getString(FORMAT);
                this.deserializationSchema = null;
            }
        }else {
            format = DEFAULT_FORMAT;
            deserializationSchema = new JsonDeserializationSchema(false, false, typeInfo);
        }
    }
}
