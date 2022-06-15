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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.sink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.state.CKCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.state.ClickhouseSinkState;

import java.io.IOException;
import java.util.Optional;

public class ClickhouseSinkWriter implements SinkWriter<SeaTunnelRow, CKCommitInfo, ClickhouseSinkState> {

    private SinkWriter.Context context;

    ClickhouseSinkWriter(SinkWriter.Context context) {
        this.context = context;
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {

    }

    @Override
    public Optional<CKCommitInfo> prepareCommit() throws IOException {
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {

    }

    @Override
    public void close() throws IOException {

    }
}
