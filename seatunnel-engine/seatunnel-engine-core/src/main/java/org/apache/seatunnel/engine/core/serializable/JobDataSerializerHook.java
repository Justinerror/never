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

package org.apache.seatunnel.engine.core.serializable;

import org.apache.seatunnel.engine.common.serializeable.SeaTunnelFactoryIdConstant;
import org.apache.seatunnel.engine.core.dag.Edge;
import org.apache.seatunnel.engine.core.dag.Vertex;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.annotation.PrivateApi;

/**
 * A Java Service Provider hook for Hazelcast's Identified Data Serializable
 * mechanism. This is private API.
 * All about the Job's data serializable define in this class.
 */
@PrivateApi
public final class JobDataSerializerHook implements DataSerializerHook {

    /**
     * Serialization ID of the {@link LogicalDag} class.
     */
    public static final int LOGICAL_DAG = 0;

    /**
     * Serialization ID of the {@link Vertex} class.
     */
    public static final int VERTEX = 1;

    /**
     * Serialization ID of the {@link Edge} class.
     */
    public static final int EDGE = 2;

    public static final int FACTORY_ID = FactoryIdHelper.getFactoryId(
        SeaTunnelFactoryIdConstant.SEATUNNEL_JOB_DATA_SERIALIZER_FACTORY,
        SeaTunnelFactoryIdConstant.SEATUNNEL_JOB_DATA_SERIALIZER_FACTORY_ID
    );

    @Override
    public int getFactoryId() {
        return FACTORY_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return new Factory();
    }

    private static class Factory implements DataSerializableFactory {
        @SuppressWarnings("checkstyle:returncount")
        @Override
        public IdentifiedDataSerializable create(int typeId) {
            switch (typeId) {
                case LOGICAL_DAG:
                    return new LogicalDag();
                case VERTEX:
                    return new Vertex();
                case EDGE:
                    return new Edge();
                default:
                    throw new IllegalArgumentException("Unknown type id " + typeId);
            }
        }
    }
}
