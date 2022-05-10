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

package org.apache.seatunnel.translation.flink.sink;

import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;

import org.apache.flink.api.connector.sink.GlobalCommitter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FlinkGlobalCommitter<CommT, GlobalCommT> implements GlobalCommitter<CommT, GlobalCommT> {

    private SinkAggregatedCommitter<CommT, GlobalCommT> aggregatedCommitter;

    FlinkGlobalCommitter(SinkAggregatedCommitter<CommT, GlobalCommT> aggregatedCommitter) {
        this.aggregatedCommitter = aggregatedCommitter;
    }

    @Override
    public List<GlobalCommT> filterRecoveredCommittables(List globalCommittables) throws IOException {
        return Collections.emptyList();
    }

    @Override
    public GlobalCommT combine(List<CommT> committables) throws IOException {
        // TODO add combine logic
        return null;
    }

    @Override
    public List<GlobalCommT> commit(List<GlobalCommT> globalCommittables) throws IOException {
        List<GlobalCommT> all = new ArrayList<>();
        globalCommittables.forEach(c -> {
            all.addAll((List) c);
        });
        globalCommittables.clear();
        globalCommittables.addAll(all);
        return aggregatedCommitter.commit(globalCommittables);
    }

    @Override
    public void endOfInput() throws IOException {
    }

    @Override
    public void close() throws Exception {
        aggregatedCommitter.close();
    }
}
