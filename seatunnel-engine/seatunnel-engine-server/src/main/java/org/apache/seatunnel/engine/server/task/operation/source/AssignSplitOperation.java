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

package org.apache.seatunnel.engine.server.task.operation.source;

import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.execution.WorkerTaskLocation;
import org.apache.seatunnel.engine.server.serializable.TaskDataSerializerHook;
import org.apache.seatunnel.engine.server.task.SourceSeaTunnelTask;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.List;

public class AssignSplitOperation<SplitT extends SourceSplit> extends Operation implements IdentifiedDataSerializable {

    private List<SplitT> splits;
    private WorkerTaskLocation taskID;

    public AssignSplitOperation() {
    }

    public AssignSplitOperation(WorkerTaskLocation taskID, List<SplitT> splits) {
        this.taskID = taskID;
        this.splits = splits;
    }

    @Override
    public void run() throws Exception {
        SeaTunnelServer server = getService();
        SourceSeaTunnelTask<?, SplitT> task =
                server.getSlotService().getSlotContext(taskID.getSlotID()).getTaskExecutionService()
                        .getExecutionContext(taskID.getTaskGroupID()).getTaskGroup().getTask(taskID.getTaskID());
        task.receivedSourceSplit(splits);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeObject(splits);
        taskID.writeData(out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        splits = in.readObject();
        taskID.readData(in);
    }

    @Override
    public int getFactoryId() {
        return TaskDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return TaskDataSerializerHook.ASSIGN_SPLIT_TYPE;
    }
}
