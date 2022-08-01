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

package org.apache.seatunnel.engine.server.dag.execution;

import org.apache.seatunnel.engine.core.dag.Edge;
import org.apache.seatunnel.engine.core.dag.Vertex;

import java.util.List;
import java.util.Map;

public class ExecutionPlan {

    private final List<Edge> edges;

    private final Map<Integer, Vertex> vertexes;

    ExecutionPlan(List<Edge> edges, Map<Integer, Vertex> vertexes) {
        this.edges = edges;
        this.vertexes = vertexes;
    }

    public List<Edge> getEdges() {
        return edges;
    }

    public Map<Integer, Vertex> getVertexes() {
        return vertexes;
    }

}
