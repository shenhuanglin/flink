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
 */

package org.apache.flink.runtime.heartbeat;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HeartbeatManagerOptions;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;

/**
 * HeartbeatServices gives access to all services needed for heartbeating. This includes the
 * creation of heartbeat receivers and heartbeat senders.
  * creation of heartbeat receivers and heartbeat senders. Note: 在 Flink
  * 的各个服务组件中，ResourceManager、JobMaster、TaskExecutor
  * 三者之间会定期发送心跳（Heartbeat）消息，以检测彼此的存活状态：ResourceManager 会主动发送心跳给 JobMaster 和 TaskExecutor，JobMaster 和
  * TaskExecutor 也会定期向 ResourceManager 发送心跳。如果某一方在规定时间内没有收到对方的心跳响应，
  * 就会认为对方已经失联，从而触发相应的故障处理机制，比如重新注册、重启组件等。 心跳的整理流程如下： 1. JobManager
  * 启动过程中，ClusterEntrypoint.initializeServices() 方法中会创建 HeartbeatServices 2. ResourceManager 第一次启动时调用
  * onStart() 方法，在其中会调用 startHeartbeatServices() 方法启动心跳服务，会创建 TaskManager、JobMaster 的心跳管理对象，
  * HeartbeatManagerSenderImpl 内部有 ConcurrentHashMap 用于监控目标，HeartbeatManagerSenderImpl 实现了 Runnable
  * 接口，其构造方法中有一个定义调度任务， 第一次启动时会立即调用 run() 方法，之后定时调用 run() 方法，该方法中会遍历所有的 HeartbeatMonitor，调用
  * requestHeartbeat() 方法发送心跳请求，告诉 JobMaster、 TaskManager 想 ResourceManager 发送心跳。 3. TaskExecutor 向
  * ResourceManager 注册时，有如下基本操作：ResourceManager 接收到注册消息会触发 HeartbeatManagerImpl 的 monitorTarget() 方法，
  * 将 TaskExecutor 信息注册到 ConcurrentHashMap 中，TaskExecutor 注册成功后，会回调 onRegistrationSuccess() 方法，
 */
public interface HeartbeatServices {

    /**
     * Creates a heartbeat manager which does not actively send heartbeats.
     *
     * @param resourceId Resource Id which identifies the owner of the heartbeat manager
     * @param heartbeatListener Listener which will be notified upon heartbeat timeouts for
     *     registered targets
     * @param mainThreadExecutor Scheduled executor to be used for scheduling heartbeat timeouts
     * @param log Logger to be used for the logging
     * @param <I> Type of the incoming payload
     * @param <O> Type of the outgoing payload
     * @return A new HeartbeatManager instance
     */
    <I, O> HeartbeatManager<I, O> createHeartbeatManager(
            ResourceID resourceId,
            HeartbeatListener<I, O> heartbeatListener,
            ScheduledExecutor mainThreadExecutor,
            Logger log);

    /**
     * Creates a heartbeat manager which actively sends heartbeats to monitoring targets.
     *
     * @param resourceId Resource Id which identifies the owner of the heartbeat manager
     * @param heartbeatListener Listener which will be notified upon heartbeat timeouts for
     *     registered targets
     * @param mainThreadExecutor Scheduled executor to be used for scheduling heartbeat timeouts and
     *     periodically send heartbeat requests
     * @param log Logger to be used for the logging
     * @param <I> Type of the incoming payload
     * @param <O> Type of the outgoing payload
     * @return A new HeartbeatManager instance which actively sends heartbeats
     */
    <I, O> HeartbeatManager<I, O> createHeartbeatManagerSender(
            ResourceID resourceId,
            HeartbeatListener<I, O> heartbeatListener,
            ScheduledExecutor mainThreadExecutor,
            Logger log);

    /**
     * Creates an HeartbeatServices instance from a {@link Configuration}.
     *
     * @param configuration Configuration to be used for the HeartbeatServices creation
     * @return An HeartbeatServices instance created from the given configuration
     */
    static HeartbeatServices fromConfiguration(Configuration configuration) {
        long heartbeatInterval = configuration.getLong(HeartbeatManagerOptions.HEARTBEAT_INTERVAL);

        long heartbeatTimeout = configuration.getLong(HeartbeatManagerOptions.HEARTBEAT_TIMEOUT);

        int failedRpcRequestsUntilUnreachable =
                configuration.get(HeartbeatManagerOptions.HEARTBEAT_RPC_FAILURE_THRESHOLD);

        return new HeartbeatServicesImpl(
                heartbeatInterval, heartbeatTimeout, failedRpcRequestsUntilUnreachable);
    }

    static HeartbeatServices noOp() {
        return NoOpHeartbeatServices.getInstance();
    }
}
