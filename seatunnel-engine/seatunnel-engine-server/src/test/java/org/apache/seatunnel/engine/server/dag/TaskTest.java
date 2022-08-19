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

package org.apache.seatunnel.engine.server.dag;

import org.apache.seatunnel.api.common.SeaTunnelContext;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSink;
import org.apache.seatunnel.connectors.seatunnel.fake.source.FakeSource;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.dag.logical.LogicalEdge;
import org.apache.seatunnel.engine.core.dag.logical.LogicalVertex;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.server.SeaTunnelNodeContext;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalPlan;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalPlanUtils;

import com.google.common.collect.Sets;
import com.hazelcast.config.Config;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.spi.impl.NodeEngine;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.concurrent.Executors;

public class TaskTest {

    private SeaTunnelServer service;

    private NodeEngine nodeEngine;

    private HazelcastInstanceImpl instance;

    @Before
    public void before() {
        Config config = new Config();
        config.setInstanceName("test");
        config.setClusterName("test");
        instance = ((HazelcastInstanceProxy) HazelcastInstanceFactory.newHazelcastInstance(config,
            "taskTest", new SeaTunnelNodeContext(new SeaTunnelConfig()))).getOriginal();
        nodeEngine = instance.node.nodeEngine;
        service = nodeEngine.getService(SeaTunnelServer.SERVICE_NAME);
    }

    @Test
    public void testTask() throws MalformedURLException {

        IdGenerator idGenerator = new IdGenerator();

        SeaTunnelContext.getContext().setJobMode(JobMode.BATCH);
        FakeSource fakeSource = new FakeSource();
        fakeSource.setSeaTunnelContext(SeaTunnelContext.getContext());

        Action fake = new SourceAction<>(idGenerator.getNextId(), "fake", fakeSource,
            Sets.newHashSet(new URL("file:///fake.jar")));
        fake.setParallelism(3);
        LogicalVertex fakeVertex = new LogicalVertex(fake.getId(), fake, 3);

        FakeSource fakeSource2 = new FakeSource();
        fakeSource2.setSeaTunnelContext(SeaTunnelContext.getContext());
        Action fake2 = new SourceAction<>(idGenerator.getNextId(), "fake", fakeSource2,
            Sets.newHashSet(new URL("file:///fake.jar")));
        fake2.setParallelism(3);
        LogicalVertex fake2Vertex = new LogicalVertex(fake2.getId(), fake2, 3);

        ConsoleSink consoleSink = new ConsoleSink();
        consoleSink.setSeaTunnelContext(SeaTunnelContext.getContext());
        Action console = new SinkAction<>(idGenerator.getNextId(), "console", consoleSink,
            Sets.newHashSet(new URL("file:///console.jar")));
        console.setParallelism(3);
        LogicalVertex consoleVertex = new LogicalVertex(console.getId(), console, 3);

        LogicalEdge edge = new LogicalEdge(fakeVertex, consoleVertex);

        LogicalDag logicalDag = new LogicalDag();
        logicalDag.addLogicalVertex(fakeVertex);
        logicalDag.addLogicalVertex(consoleVertex);
        logicalDag.addLogicalVertex(fake2Vertex);
        logicalDag.addEdge(edge);

        JobConfig config = new JobConfig();
        config.setName("test");
        config.setMode(JobMode.BATCH);

        JobImmutableInformation jobImmutableInformation = new JobImmutableInformation(1,
            nodeEngine.getSerializationService().toData(logicalDag), config, Collections.emptyList());

        PassiveCompletableFuture<Void> voidPassiveCompletableFuture =
            service.submitJob(jobImmutableInformation.getJobId(),
                nodeEngine.getSerializationService().toData(jobImmutableInformation));

        Assert.assertNotNull(voidPassiveCompletableFuture);
    }

    @Test
    public void testLogicalToPhysical() throws MalformedURLException {

        IdGenerator idGenerator = new IdGenerator();

        Action fake = new SourceAction<>(idGenerator.getNextId(), "fake", new FakeSource(),
            Sets.newHashSet(new URL("file:///fake.jar")));
        LogicalVertex fakeVertex = new LogicalVertex(fake.getId(), fake, 2);

        Action fake2 = new SourceAction<>(idGenerator.getNextId(), "fake", new FakeSource(),
            Sets.newHashSet(new URL("file:///fake.jar")));
        LogicalVertex fake2Vertex = new LogicalVertex(fake2.getId(), fake2, 2);

        Action console = new SinkAction<>(idGenerator.getNextId(), "console", new ConsoleSink(),
            Sets.newHashSet(new URL("file:///console.jar")));
        LogicalVertex consoleVertex = new LogicalVertex(console.getId(), console, 2);

        LogicalEdge edge = new LogicalEdge(fakeVertex, consoleVertex);

        LogicalDag logicalDag = new LogicalDag();
        logicalDag.addLogicalVertex(fakeVertex);
        logicalDag.addLogicalVertex(consoleVertex);
        logicalDag.addLogicalVertex(fake2Vertex);
        logicalDag.addEdge(edge);

        JobConfig config = new JobConfig();
        config.setName("test");
        config.setMode(JobMode.BATCH);

        JobImmutableInformation jobImmutableInformation = new JobImmutableInformation(1,
            nodeEngine.getSerializationService().toData(logicalDag), config, Collections.emptyList());

        PhysicalPlan physicalPlan = PhysicalPlanUtils.fromLogicalDAG(logicalDag, nodeEngine,
            jobImmutableInformation,
            System.currentTimeMillis(),
            Executors.newCachedThreadPool(),
            instance.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME));

        Assert.assertEquals(physicalPlan.getPipelineList().size(), 1);
        Assert.assertEquals(physicalPlan.getPipelineList().get(0).getCoordinatorVertexList().size(), 1);
        Assert.assertEquals(physicalPlan.getPipelineList().get(0).getPhysicalVertexList().size(), 1);
    }

    @After
    public void after() {
        instance.shutdown();
    }
}
