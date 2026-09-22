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

# 用于启动或停止一个 TaskManager 实例
# Start/stop a Flink TaskManager.
USAGE="Usage: taskmanager.sh (start|start-foreground|stop|stop-all)"

# 获取调用脚本时传入的操作命令参数
STARTSTOP=$1

# 将第二个参数开始的所有参数收入 ARGS 数组，作为 JVM 参数传递给 TM 实例
ARGS=("${@:2}")

# 参数校验
if [[ $STARTSTOP != "start" ]] && [[ $STARTSTOP != "start-foreground" ]] && [[ $STARTSTOP != "stop" ]] && [[ $STARTSTOP != "stop-all" ]]; then
  echo $USAGE
  exit 1
fi

# 获取绝对路径并加载配置
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

# 设置入口点，flink-daemon.sh 会将其映射到 Java 主类 TaskManagerRunner
ENTRYPOINT=taskexecutor

# 构建启动参数
if [[ $STARTSTOP == "start" ]] || [[ $STARTSTOP == "start-foreground" ]]; then

    # 如果用户既没有设置通用 JVM 参数 FLINK_ENV_JAVA_OPTS，也没有设置 TM 特定的 JVM 参数
    # FLINK_ENV_JAVA_OPTS_TM，则默认启用 G1 垃圾回收器，这是 TM 性能优化的默认值，提供更
    # 好的性能和更低的停顿时间
    # if no other JVM options are set, set the GC to G1
    if [ -z "${FLINK_ENV_JAVA_OPTS}" ] && [ -z "${FLINK_ENV_JAVA_OPTS_TM}" ]; then
        export JVM_ARGS="$JVM_ARGS -XX:+UseG1GC"
    fi

    # 将 TM 特定的 JVM 参数追加到通用 JVM 参数后面
    # Add TaskManager-specific JVM options
    export FLINK_ENV_JAVA_OPTS="${FLINK_ENV_JAVA_OPTS} ${FLINK_ENV_JAVA_OPTS_TM}"

    # Startup parameters

    # 计算 TM 的内存参数并追加到 DYNAMIC_PARAMETERS 变量中
    parseTmArgsAndExportLogs "${ARGS[@]}"

    # 将内存动态参数前置到 ARGS 数组头部，确保它们在命令行参数中优先传递给 TM 实例
    if [ ! -z "${DYNAMIC_PARAMETERS}" ]; then
        ARGS=(${DYNAMIC_PARAMETERS[@]} "${ARGS[@]}")
    fi

    # 添加配置文件目录
    ARGS=("--configDir" "${FLINK_CONF_DIR}" "${ARGS[@]}")
fi

# 前台运行
if [[ $STARTSTOP == "start-foreground" ]]; then
    exec "${FLINK_BIN_DIR}"/flink-console.sh $ENTRYPOINT "${ARGS[@]}"
# 后台启动 TM 实例
else
    # 如果 FLINK_TM_COMPUTE_NUMA 环境变量设置为 false，则直接启动一个 TM 实例，
    # 否则对每个 NUMA 节点，绑定内存和 CPU，启动一个独立的 TM 实例，实现 NUMA 感知的内存局部性优化
    if [[ $FLINK_TM_COMPUTE_NUMA == "false" ]]; then
        # Start a single TaskManager
        "${FLINK_BIN_DIR}"/flink-daemon.sh $STARTSTOP $ENTRYPOINT "${ARGS[@]}"
    else
        # Example output from `numactl --show` on an AWS c4.8xlarge:
        # policy: default
        # preferred node: current
        # physcpubind: 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35
        # cpubind: 0 1
        # nodebind: 0 1
        # membind: 0 1
        read -ra NODE_LIST <<< $(numactl --show | grep "^nodebind: ")
        for NODE_ID in "${NODE_LIST[@]:1}"; do
            # Start a TaskManager for each NUMA node
            numactl --membind=$NODE_ID --cpunodebind=$NODE_ID -- "${FLINK_BIN_DIR}"/flink-daemon.sh $STARTSTOP $ENTRYPOINT "${ARGS[@]}"
        done
    fi
fi
