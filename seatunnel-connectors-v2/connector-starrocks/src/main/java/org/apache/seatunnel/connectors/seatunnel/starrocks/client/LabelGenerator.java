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

package org.apache.seatunnel.connectors.seatunnel.starrocks.client;

import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig;

import java.util.UUID;

/** Generator label for stream load. */
public class LabelGenerator {
    private String labelPrefix;
    private boolean enable2PC;

    public LabelGenerator(SinkConfig sinkConfig) {
        this.labelPrefix = sinkConfig.getLabelPrefix();
        this.enable2PC = sinkConfig.isEnable2PC();
    }

    public String genLabel(long chkId, int subTaskIndex) {
        return enable2PC
                ? labelPrefix + "_" + chkId + "_" + subTaskIndex
                : labelPrefix + "_" + UUID.randomUUID();
    }

    public String genLabel() {
        return labelPrefix + "_" + UUID.randomUUID();
    }
}
