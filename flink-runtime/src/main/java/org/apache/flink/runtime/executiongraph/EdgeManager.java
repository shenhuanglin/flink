/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ConsumerVertexGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Class that manages all the connections between tasks. */
public class EdgeManager {

    // 分区 ID 与消费该分区的所有 ConsumerVertexGroup 的映射。一个 ConsumerVertexGroup 中的 Vertex 必须属于
    // 同一个 ExecutionJobVertex，如果有侧输出流或者拓扑分叉，则一个上游 ExecutionJobVertex 会有多个下游
    // ExecutionJobVertex，因此一个分区可能会被多个 ConsumerVertexGroup 消费
    private final Map<IntermediateResultPartitionID, List<ConsumerVertexGroup>> partitionConsumers =
            new HashMap<>();

    // ExecutionVertex ID 与该 Vertex 消费的所有 ConsumedPartitionGroup 的映射。一个 ExecutionVertex
    // 可能同时接收来自多个上游 ConsumedPartitionGroup 的数据，如果不使用 List，则无法实现双流 Join 或者
    // 多流 Union
    private final Map<ExecutionVertexID, List<ConsumedPartitionGroup>> vertexConsumedPartitions =
            new HashMap<>();

    // 分区 ID 与包含该分区的所有 ConsumedPartitionGroup 之间的映射，主要用于实现状态变更的通知
    private final Map<IntermediateResultPartitionID, List<ConsumedPartitionGroup>>
            consumedPartitionsById = new HashMap<>();

    public void connectPartitionWithConsumerVertexGroup(
            IntermediateResultPartitionID resultPartitionId,
            ConsumerVertexGroup consumerVertexGroup) {

        checkNotNull(consumerVertexGroup);

        List<ConsumerVertexGroup> groups =
                getConsumerVertexGroupsForPartitionInternal(resultPartitionId);
        groups.add(consumerVertexGroup);
    }

    public void connectVertexWithConsumedPartitionGroup(
            ExecutionVertexID executionVertexId, ConsumedPartitionGroup consumedPartitionGroup) {

        checkNotNull(consumedPartitionGroup);

        final List<ConsumedPartitionGroup> consumedPartitions =
                getConsumedPartitionGroupsForVertexInternal(executionVertexId);

        consumedPartitions.add(consumedPartitionGroup);
    }

    private List<ConsumerVertexGroup> getConsumerVertexGroupsForPartitionInternal(
            IntermediateResultPartitionID resultPartitionId) {
        return partitionConsumers.computeIfAbsent(resultPartitionId, id -> new ArrayList<>());
    }

    private List<ConsumedPartitionGroup> getConsumedPartitionGroupsForVertexInternal(
            ExecutionVertexID executionVertexId) {
        return vertexConsumedPartitions.computeIfAbsent(executionVertexId, id -> new ArrayList<>());
    }

    public List<ConsumerVertexGroup> getConsumerVertexGroupsForPartition(
            IntermediateResultPartitionID resultPartitionId) {
        return Collections.unmodifiableList(
                getConsumerVertexGroupsForPartitionInternal(resultPartitionId));
    }

    public List<ConsumedPartitionGroup> getConsumedPartitionGroupsForVertex(
            ExecutionVertexID executionVertexId) {
        return Collections.unmodifiableList(
                getConsumedPartitionGroupsForVertexInternal(executionVertexId));
    }

    public void registerConsumedPartitionGroup(ConsumedPartitionGroup group) {
        for (IntermediateResultPartitionID partitionId : group) {
            consumedPartitionsById
                    .computeIfAbsent(partitionId, ignore -> new ArrayList<>())
                    .add(group);
        }
    }

    private List<ConsumedPartitionGroup> getConsumedPartitionGroupsByIdInternal(
            IntermediateResultPartitionID resultPartitionId) {
        return consumedPartitionsById.computeIfAbsent(resultPartitionId, id -> new ArrayList<>());
    }

    public List<ConsumedPartitionGroup> getConsumedPartitionGroupsById(
            IntermediateResultPartitionID resultPartitionId) {
        return Collections.unmodifiableList(
                getConsumedPartitionGroupsByIdInternal(resultPartitionId));
    }

    public int getNumberOfConsumedPartitionGroupsById(
            IntermediateResultPartitionID resultPartitionId) {
        return getConsumedPartitionGroupsByIdInternal(resultPartitionId).size();
    }
}
