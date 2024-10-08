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

package org.apache.seatunnel.connectors.seatunnel.file.oss.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSystemType;
import org.apache.seatunnel.connectors.seatunnel.file.oss.config.OssHadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.sink.BaseMultipleTableFileSink;

public class OssFileSink extends BaseMultipleTableFileSink {

    private final CatalogTable catalogTable;

    public OssFileSink(ReadonlyConfig readonlyConfig, CatalogTable catalogTable) {
        super(OssHadoopConf.buildWithConfig(readonlyConfig), readonlyConfig, catalogTable);
        this.catalogTable = catalogTable;
    }

    @Override
    public String getPluginName() {
        return FileSystemType.OSS.getFileSystemPluginName();
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.ofNullable(catalogTable);
    }
}
