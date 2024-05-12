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

package org.apache.seatunnel.translation.spark.source.partition.batch;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.source.BaseSourceFunction;
import org.apache.seatunnel.translation.source.CoordinatedSource;
import org.apache.seatunnel.translation.spark.serialization.InternalRowCollector;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class CoordinatedBatchPartitionReader extends ParallelBatchPartitionReader {

    protected final Map<Integer, InternalRowCollector> collectorMap;
    private static final long SLEEP_TIME_MS = 100L;
    private static final long SLEEP_TIME_0_MS = 0L;

    public CoordinatedBatchPartitionReader(
            SeaTunnelSource<SeaTunnelRow, ?, ?> source,
            Integer parallelism,
            String jobId,
            Integer subtaskId,
            Map<String, String> envOptions) {
        super(source, parallelism, jobId, subtaskId, envOptions);
        this.collectorMap = new HashMap<>(parallelism);
        for (int i = 0; i < parallelism; i++) {
            collectorMap.put(
                    i,
                    new InternalRowCollector(
                            handover, new Object(), source.getProducedType(), envOptions));
        }
    }

    @Override
    protected String getEnumeratorThreadName() {
        return "coordinated-split-enumerator-executor";
    }

    @Override
    protected BaseSourceFunction<SeaTunnelRow> createInternalSource() {
        return new InternalCoordinatedSource<>(source, null, parallelism, jobId);
    }

    public class InternalCoordinatedSource<SplitT extends SourceSplit, StateT extends Serializable>
            extends CoordinatedSource<SeaTunnelRow, SplitT, StateT> {

        public InternalCoordinatedSource(
                SeaTunnelSource<SeaTunnelRow, SplitT, StateT> source,
                Map<Integer, List<byte[]>> restoredState,
                int parallelism,
                String jobId) {
            super(source, restoredState, parallelism, jobId);
        }

        @Override
        public void run(Collector<SeaTunnelRow> collector) throws Exception {
            readerMap
                    .entrySet()
                    .parallelStream()
                    .forEach(
                            entry -> {
                                final AtomicBoolean flag = readerRunningMap.get(entry.getKey());
                                final SourceReader<SeaTunnelRow, SplitT> reader = entry.getValue();
                                final Collector<SeaTunnelRow> rowCollector =
                                        collectorMap.get(entry.getKey());
                                executorService.execute(
                                        () -> {
                                            while (flag.get()) {
                                                try {
                                                    reader.pollNext(rowCollector);
                                                    if (rowCollector.isEmptyThisPollNext()) {
                                                        Thread.sleep(SLEEP_TIME_MS);
                                                    } else {
                                                        rowCollector.resetEmptyThisPollNext();
                                                        /**
                                                         * sleep(0) is used to prevent the current
                                                         * thread from occupying CPU resources for a
                                                         * long time, thus blocking the checkpoint
                                                         * thread for a long time. It is mentioned
                                                         * in this
                                                         * https://github.com/apache/seatunnel/issues/5694
                                                         */
                                                        Thread.sleep(SLEEP_TIME_0_MS);
                                                    }
                                                } catch (Exception e) {
                                                    this.running = false;
                                                    flag.set(false);
                                                    throw new RuntimeException(e);
                                                }
                                            }
                                        });
                            });
            splitEnumerator.run();
            while (this.running) {
                Thread.sleep(SLEEP_TIME_INTERVAL);
            }
        }

        @Override
        protected void handleNoMoreElement(int subtaskId) {
            super.handleNoMoreElement(subtaskId);
            if (!this.running) {
                CoordinatedBatchPartitionReader.this.running = false;
            }
        }
    }
}
