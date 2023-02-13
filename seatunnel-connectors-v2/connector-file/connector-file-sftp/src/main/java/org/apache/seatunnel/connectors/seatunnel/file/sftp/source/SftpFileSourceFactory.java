/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.seatunnel.connectors.seatunnel.file.sftp.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.connectors.seatunnel.common.schema.SeaTunnelSchema;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSystemType;
import org.apache.seatunnel.connectors.seatunnel.file.sftp.config.SftpConfig;

import com.google.auto.service.AutoService;

import java.util.Arrays;

@AutoService(Factory.class)
public class SftpFileSourceFactory implements TableSourceFactory {
    
    @Override
    public String factoryIdentifier() {
        return FileSystemType.SFTP.getFileSystemPluginName();
    }
    
    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(SftpConfig.FILE_PATH)
                .required(SftpConfig.SFTP_HOST)
                .required(SftpConfig.SFTP_PORT)
                .required(SftpConfig.SFTP_USERNAME)
                .required(SftpConfig.SFTP_PASSWORD)
                .required(BaseSourceConfig.FILE_TYPE)
                .conditional(
                        BaseSourceConfig.FILE_TYPE, FileFormat.TEXT, BaseSourceConfig.DELIMITER)
                .conditional(
                        BaseSourceConfig.FILE_TYPE,
                        Arrays.asList(FileFormat.TEXT, FileFormat.JSON),
                        SeaTunnelSchema.SCHEMA)
                .optional(BaseSourceConfig.PARSE_PARTITION_FROM_PATH)
                .optional(BaseSourceConfig.DATE_FORMAT)
                .optional(BaseSourceConfig.DATETIME_FORMAT)
                .optional(BaseSourceConfig.TIME_FORMAT)
                .build();
    }
    
    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return SftpFileSource.class;
    }
}
