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

# 用于启动或停止一个 JobManager 实例
# Start/stop a Flink JobManager.
USAGE="Usage: jobmanager.sh ((start|start-foreground) [args])|stop|stop-all"

# 第一个参数，取值可以是 start、start-foreground、stop 或 stop-all，分别表示以后台守护进程方式启动 JM、
# 以当前控制台应用方式启动 JM、停止当前 JM 实例和停止所有 JM 实例
STARTSTOP=$1

# 如果第二个参数为空或者第二个参数以 -D 开头（动态参数），则说明没有指定主机和端口参数，直接将后续参数收入 args 数组
# 作为 JVM 参数传递给 JM 实例
if [ -z $2 ] || [[ $2 == "-D" ]]; then
    # start [-D ...]
    args=("${@:2}")
# 如果第三个参数为空或者第三个参数以 -D 开头，则说明指定了主机参数但没有指定端口参数，将第二个参数作为主机参数，
# 后续参数收入 args 数组
elif [ -z $3 ] || [[ $3 == "-D" ]]; then
    # legacy path: start <host> [-D ...]
    HOST=$2
    args=("${@:3}")
# 否则就是同时传了主机和端口参数，将第二个参数作为主机参数，第三个参数作为端口参数，后续参数收入 args 数组
else
    # legacy path: start <host> <port> [-D ...]
    HOST=$2
    WEBUIPORT=$3
    args=("${@:4}")
fi

# 检验参数，如果第一个参数不是合法值，则输出使用说明并退出
if [[ $STARTSTOP != "start" ]] && [[ $STARTSTOP != "start-foreground" ]] && [[ $STARTSTOP != "stop" ]] && [[ $STARTSTOP != "stop-all" ]]; then
  echo $USAGE
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

# 设置入口点
ENTRYPOINT=standalonesession

if [[ $STARTSTOP == "start" ]] || [[ $STARTSTOP == "start-foreground" ]]; then
    # 将 JM 特定的 JVM 参数追加到通用 JVM 参数后面并 export 使子进程可见
    # Add JobManager-specific JVM options
    export FLINK_ENV_JAVA_OPTS="${FLINK_ENV_JAVA_OPTS} ${FLINK_ENV_JAVA_OPTS_JM}"
    # 计算 JM 的内存参数并追加到 DYNAMIC_PARAMETERS 变量中，在后续传递给 JM 实例的命令行参数中使用
    parseJmArgsAndExportLogs "${args[@]}"

    # 设置日志相关参数
    args=("--configDir" "${FLINK_CONF_DIR}" "--executionMode" "cluster" "${args[@]}")
    # 若 --host 参数非空，则将其和对应的值追加到 args 数组中
    if [ ! -z $HOST ]; then
        args+=("--host")
        args+=("${HOST}")
    fi

    # 如果--webui-port 参数非空，则将其和对应的值追加到 args 数组中
    if [ ! -z $WEBUIPORT ]; then
        args+=("--webui-port")
        args+=("${WEBUIPORT}")
    fi

    # 如果 DYNAMIC_PARAMETERS 变量非空，则将其中的参数追加到 args 数组的前面，确保动态参数优先于其他参数
    if [ ! -z "${DYNAMIC_PARAMETERS}" ]; then
        args=(${DYNAMIC_PARAMETERS[@]} "${args[@]}")
    fi
fi

# 如果使用前台控制台应用方式启动 JM，用 exec 替换当前 shell 进程，调用 flink-console.sh 脚本启动 JM 实例（日志
# 直接输出在终端，常用于调试），exec 不创建子进程，而是直接在当前进程中执行命令，命令执行完成后当前进程结束
if [[ $STARTSTOP == "start-foreground" ]]; then
    exec "${FLINK_BIN_DIR}"/flink-console.sh $ENTRYPOINT "${args[@]}"
# 其余指令均采用后台守护进程方式启动或停止 JM 实例，调用 flink-daemon.sh 脚本，它会在后台启动一个新的 JVM 进程来
# 运行 JM 实例，并将日志输出到指定的日志文件中
else
    "${FLINK_BIN_DIR}"/flink-daemon.sh $STARTSTOP $ENTRYPOINT "${args[@]}"
fi
