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

package org.apache.seatunnel.connectors.seatunnel.starrocks.sink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.starrocks.client.LabelGenerator;
import org.apache.seatunnel.connectors.seatunnel.starrocks.client.StarRocksSinkManager;
import org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.StarRocksSinkManagerV2;
import org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.StreamLoadManager;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorException;
import org.apache.seatunnel.connectors.seatunnel.starrocks.serialize.StarRocksCsvSerializer;
import org.apache.seatunnel.connectors.seatunnel.starrocks.serialize.StarRocksISerializer;
import org.apache.seatunnel.connectors.seatunnel.starrocks.serialize.StarRocksJsonSerializer;
import org.apache.seatunnel.connectors.seatunnel.starrocks.serialize.StarRocksSinkOP;
import org.apache.seatunnel.connectors.seatunnel.starrocks.sink.committer.StarRocksCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.starrocks.sink.state.StarRocksSinkState;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class StarRocksSinkWriter
        implements SinkWriter<SeaTunnelRow, StarRocksCommitInfo, StarRocksSinkState> {

    private final StarRocksISerializer serializer;
    private final StreamLoadManager manager;
    private SinkConfig sinkConfig;
    private long lastCheckpointId;
    private LabelGenerator labelGenerator;

    public StarRocksSinkWriter(
            SinkWriter.Context context,
            SinkConfig sinkConfig,
            SeaTunnelRowType seaTunnelRowType,
            List<StarRocksSinkState> starRocksSinkStates) {

        this.sinkConfig = sinkConfig;
        List<String> fieldNames =
                Arrays.stream(seaTunnelRowType.getFieldNames()).collect(Collectors.toList());
        if (sinkConfig.isEnableUpsertDelete()) {
            fieldNames.add(StarRocksSinkOP.COLUMN_KEY);
        }
        labelGenerator = new LabelGenerator(sinkConfig);
        this.serializer = createSerializer(sinkConfig, seaTunnelRowType);
        this.manager =
                sinkConfig.isEnable2PC()
                        ? new StarRocksSinkManagerV2(labelGenerator, sinkConfig, context)
                        : new StarRocksSinkManager(labelGenerator, sinkConfig, fieldNames);
        manager.init();
        this.lastCheckpointId =
                starRocksSinkStates.size() != 0 ? starRocksSinkStates.get(0).getCheckpointId() : 0;

        if (sinkConfig.isEnable2PC()) {
            // restore to re commit transaction
            if (!starRocksSinkStates.isEmpty()) {
                log.info("restore checkpointId {}", lastCheckpointId);
                // abort all transaction number bigger than current transaction, because they maybe
                // already start
                //  transaction.
                try {
                    for (StarRocksSinkState state : starRocksSinkStates) {

                        log.info(
                                "abort prev transaction , start lastCheckpointId : {}, getSubTaskIndex : {}",
                                lastCheckpointId + 1,
                                context.getIndexOfSubtask());
                        manager.abort(lastCheckpointId + 1, context.getIndexOfSubtask());
                    }

                } catch (Exception e) {
                    throw new StarRocksConnectorException(
                            StarRocksConnectorErrorCode.FLUSH_DATA_FAILED, e);
                }

            }
            manager.beginTransaction(lastCheckpointId + 1);
        }
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        String record;
        try {
            record = serializer.serialize(element);
        } catch (Exception e) {
            throw new StarRocksConnectorException(
                    CommonErrorCode.WRITER_OPERATION_FAILED,
                    "serialize failed. Row={" + element + "}",
                    e);
        }
        manager.write(record);
    }

    @SneakyThrows
    @Override
    public Optional<StarRocksCommitInfo> prepareCommit() {
        if (!sinkConfig.isEnable2PC()) {
            return Optional.empty();
        }
        // Flush to storage before snapshot state is performed
        return manager.prepareCommit();
    }

    @Override
    public void abortPrepare() throws Exception {
        if (sinkConfig.isEnable2PC()) {
            manager.abort();
        }
    }

    @Override
    public List<StarRocksSinkState> snapshotState(long checkpointId) throws IOException {
        if (sinkConfig.isEnable2PC()) {
            return manager.snapshot(checkpointId);
        }
        return Collections.emptyList();
    }

    @Override
    public void close() throws IOException {
        try {
            if (manager != null) {
                manager.close();
            }
        } catch (IOException e) {
            log.error("Close starRocks manager failed.", e);
            throw new StarRocksConnectorException(CommonErrorCode.WRITER_OPERATION_FAILED, e);
        }
    }

    public static StarRocksISerializer createSerializer(
            SinkConfig sinkConfig, SeaTunnelRowType seaTunnelRowType) {
        if (SinkConfig.StreamLoadFormat.CSV.equals(sinkConfig.getLoadFormat())) {
            return new StarRocksCsvSerializer(
                    sinkConfig.getColumnSeparator(),
                    seaTunnelRowType,
                    sinkConfig.isEnableUpsertDelete());
        }
        if (SinkConfig.StreamLoadFormat.JSON.equals(sinkConfig.getLoadFormat())) {
            return new StarRocksJsonSerializer(seaTunnelRowType, sinkConfig.isEnableUpsertDelete());
        }
        throw new StarRocksConnectorException(
                CommonErrorCode.ILLEGAL_ARGUMENT,
                "Failed to create row serializer, unsupported `format` from stream load properties.");
    }
}
