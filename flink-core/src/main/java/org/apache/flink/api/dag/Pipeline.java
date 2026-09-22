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
 * limitations under the License.
 *
 */

package org.apache.flink.api.dag;

import org.apache.flink.annotation.Internal;

// 在 Flink 中，Pipeline 是一个核心抽象接口，代表了数据处理任务的逻辑结构和执行计划。它定义了数据流图的拓扑结构，包括数据源、转换操作和数据汇等组件。用于描述数据处理任务的整体流程。
// 无论是 DataStream API 还是 Table API，最终都会被转换为 Pipeline，然后传递给 PipelineExecutor，将其转换成 JobGraph。
/** Common interface for Flink pipelines. */
@Internal
public interface Pipeline {}
