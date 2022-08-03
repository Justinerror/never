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

package org.apache.seatunnel.core.starter.spark.utils;

import org.apache.seatunnel.core.starter.spark.args.SparkCommandArgs;

import com.beust.jcommander.ParameterException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CommandLineUtilsTest {

    @Test
    public void testParseSparkArgs() {
        String[] args = {"-c", "app.conf", "-e", "cluster", "-m", "local[*]"};
        SparkCommandArgs commandLineArgs = CommandLineUtils.parseSparkArgs(args);

        Assertions.assertEquals("app.conf", commandLineArgs.getConfigFile());
        Assertions.assertEquals("cluster", commandLineArgs.getDeployMode().getName());
    }

    @Test
    public void testParseSparkArgsException() {
        String[] args = {"-c", "app.conf", "-e", "cluster2xxx", "-m", "local[*]"};
        Assertions.assertThrows(ParameterException.class, () -> CommandLineUtils.parseSparkArgs(args));
    }
}
