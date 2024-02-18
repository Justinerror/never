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

package org.apache.seatunnel.e2e.sink.inmemory;

import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class InMemorySinkWriter
        implements SinkWriter<SeaTunnelRow, InMemoryCommitInfo, InMemoryState>,
                SupportMultiTableSinkWriter<InMemoryConnection> {

    private static final List<String> events = new ArrayList<>();
    private static final List<InMemoryMultiTableResourceManager> resourceManagers =
            new ArrayList<>();

    public static List<String> getEvents() {
        return events;
    }

    public static List<InMemoryMultiTableResourceManager> getResourceManagers() {
        return resourceManagers;
    }

    private InMemoryMultiTableResourceManager resourceManager;

    @Override
    public void write(SeaTunnelRow element) throws IOException {}

    @Override
    public Optional<InMemoryCommitInfo> prepareCommit() throws IOException {
        return Optional.of(new InMemoryCommitInfo());
    }

    @Override
    public void abortPrepare() {}

    @Override
    public void close() throws IOException {}

    @Override
    public Optional<Integer> primaryKey() {
        return Optional.of(0);
    }

    @Override
    public MultiTableResourceManager<InMemoryConnection> initMultiTableResourceManager(
            int tableSize, int queueSize) {
        events.add("initMultiTableResourceManager" + queueSize);
        return new InMemoryMultiTableResourceManager();
    }

    @Override
    public void setMultiTableResourceManager(
            MultiTableResourceManager<InMemoryConnection> multiTableResourceManager,
            int queueIndex) {
        events.add("setMultiTableResourceManager" + queueIndex);
        this.resourceManager = (InMemoryMultiTableResourceManager) multiTableResourceManager;
        resourceManagers.add(resourceManager);
    }
}
