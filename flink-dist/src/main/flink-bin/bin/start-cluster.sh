#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

# 获取当前脚本所在的目录路径
bin=`dirname "$0"`
# 进入目录后输出绝对路径并赋值
bin=`cd "$bin"; pwd`

# . 等价于 source，表示在当前 shell 进程中执行 config.sh 脚本，加载配置文件中的环境变量和函数定义
. "$bin"/config.sh

# 开启大小写不敏感匹配模式，允许在 [[ ]] 中的字符串比较忽略大小写差异
# Start the JobManager instance(s)
shopt -s nocasematch
# 启动 JobManager 实例，分为高可用模式和 Standalone 模式，通过执行 jobmanager.sh 脚本实现
# 判断是否是高可用模式
if [[ $HIGH_AVAILABILITY == "zookeeper" ]]; then
    # 执行配置文件中的 readMasters 函数，它读取 conf/masters 文件中的主机列表，并将其存储在 MASTERS 数组中，
    # 同时将对应的 Web UI 端口存储在 WEBUIPORTS 数组中，如果端口未指定，默认填 0，并检查所有 master 是否配置
    # 为 localhost，结果存入 MASTERS_ALL_LOCALHOST 变量中（布尔值）
    # HA Mode
    readMasters

    # 打印集群中 master 节点的数量
    echo "Starting HA cluster with ${#MASTERS[@]} masters."

    for ((i=0;i<${#MASTERS[@]};++i)); do
        master=${MASTERS[i]}
        webuiport=${WEBUIPORTS[i]}

        # 如果所有 master 都配置为 localhost，则直接在本地调用 jobmanager.sh 脚本启动 JobManager 实例，否则
        # 通过 ssh 连接到远程主机并在后台启动 JobManager 实例
        if [ ${MASTERS_ALL_LOCALHOST} = true ] ; then
            "${FLINK_BIN_DIR}"/jobmanager.sh start "${master}" "${webuiport}"
        else
            ssh -n $FLINK_SSH_OPTS $master -- "nohup /bin/bash -l \"${FLINK_BIN_DIR}/jobmanager.sh\" start ${master} ${webuiport} &"
        fi
    done

# 非高可用（单 JobManager）模式
else
    echo "Starting cluster."

    # 在本机直接启动单个 JobManager，不传递主机和端口参数，使用默认配置
    # Start single JobManager on this machine
    "$FLINK_BIN_DIR"/jobmanager.sh start
fi
# 关闭大小写不敏感匹配模式，恢复默认的字符串比较行为
shopt -u nocasematch

# 调用配置文件中的 TMWorkers 函数，传入参数 start，它会调用 readWorkers 读取 conf/workers 文件中的 worker
# 节点列表，将每行的主机名存储在 WORKERS 数组中，并判断是否全为本地主机，结果存储在 WORKERS_ALL_LOCALHOST 变量中。
# 然后根据 WORKERS_ALL_LOCALHOST 的值，决定是在本地调用 taskmanager.sh 脚本启动 TaskManager 实例还是通过 ssh
# 连接到远程主机启动 TaskManager 实例。每个 worker 都会启动一个独立的 TaskManager 进程
# Start TaskManager instance(s)
TMWorkers start
