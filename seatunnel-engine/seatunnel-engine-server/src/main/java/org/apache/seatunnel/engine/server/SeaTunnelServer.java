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

package org.apache.seatunnel.engine.server;

import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.server.master.JobMaster;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.SimpleResourceManager;
import org.apache.seatunnel.engine.server.service.slot.DefaultSlotService;
import org.apache.seatunnel.engine.server.service.slot.SlotService;

import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.services.MembershipAwareService;
import com.hazelcast.internal.services.MembershipServiceEvent;
import com.hazelcast.jet.impl.LiveOperationRegistry;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.LiveOperations;
import com.hazelcast.spi.impl.operationservice.LiveOperationsTracker;
import lombok.NonNull;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SeaTunnelServer implements ManagedService, MembershipAwareService, LiveOperationsTracker {
    private static final ILogger LOGGER = Logger.getLogger(SeaTunnelServer.class);
    public static final String SERVICE_NAME = "st:impl:seaTunnelServer";

    private NodeEngineImpl nodeEngine;
    private final ILogger logger;
    private final LiveOperationRegistry liveOperationRegistry;

    private SlotService slotService;

    private final ExecutorService executorService;
    private ResourceManager resourceManager;

    private final SeaTunnelConfig seaTunnelConfig;

    private Map<Long, JobMaster> runningJobMasterMap = new ConcurrentHashMap<>();

    public SeaTunnelServer(@NonNull Node node, @NonNull SeaTunnelConfig seaTunnelConfig) {
        this.logger = node.getLogger(getClass());
        this.liveOperationRegistry = new LiveOperationRegistry();
        this.seaTunnelConfig = seaTunnelConfig;
        this.executorService =
                Executors.newFixedThreadPool(seaTunnelConfig.getEngineConfig().getServerExecutorPoolSize());
        logger.info("SeaTunnel server start...");
    }

    public SlotService getSlotService() {
        return this.slotService;
    }

    @Override
    public void init(NodeEngine engine, Properties hzProperties) {
        this.nodeEngine = (NodeEngineImpl) engine;
        // TODO Determine whether to create a SlotService on the master node according to the deploy type
        this.slotService = new DefaultSlotService(nodeEngine, false, 2);
        if (nodeEngine.getClusterService().isMaster()) {
            resourceManager = new SimpleResourceManager();
        }
    }

    @Override
    public void reset() {

    }

    @Override
    public void shutdown(boolean terminate) {
        slotService.close();
    }

    @Override
    public void memberAdded(MembershipServiceEvent event) {

    }

    @Override
    public void memberRemoved(MembershipServiceEvent event) {

    }

    @Override
    public void populate(LiveOperations liveOperations) {

    }

    /**
     * Used for debugging on call
     */
    public String printMessage(String message) {
        this.logger.info(nodeEngine.getThisAddress() + ":" + message);
        return message;
    }

    public LiveOperationRegistry getLiveOperationRegistry() {
        return liveOperationRegistry;
    }

    public ResourceManager getResourceManager() {
        return resourceManager;
    }

    /**
     * call by client to submit job
     */
    public PassiveCompletableFuture<Void> submitJob(long jobId, Data jobImmutableInformation) {
        CompletableFuture<Void> voidCompletableFuture = new CompletableFuture<>();
        JobMaster jobMaster = new JobMaster(jobImmutableInformation, this.nodeEngine, executorService, resourceManager);
        executorService.submit(() -> {
            try {
                jobMaster.init();
                runningJobMasterMap.put(jobId, jobMaster);
            } catch (Throwable e) {
                LOGGER.severe(String.format("submit job %s error %s ", jobId, ExceptionUtils.getMessage(e)));
                voidCompletableFuture.completeExceptionally(e);
            } finally {
                // We specify that when init is complete, the submitJob is complete
                voidCompletableFuture.complete(null);
            }

            try {
                jobMaster.run();
            } finally {
                runningJobMasterMap.remove(jobId);
            }
        });
        return new PassiveCompletableFuture(voidCompletableFuture);
    }

    public PassiveCompletableFuture<JobStatus> waitForJobComplete(long jobId) {
        JobMaster runningJobMaster = runningJobMasterMap.get(jobId);
        if (runningJobMaster == null) {
            // TODO Get Job Status from JobHistoryStorage
            CompletableFuture<JobStatus> future = new CompletableFuture<>();
            future.complete(JobStatus.FINISHED);
            return new PassiveCompletableFuture<>(future);
        } else {
            return runningJobMaster.getJobMasterCompleteFuture();
        }
    }
}
