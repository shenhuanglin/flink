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

# 真正执行 JVM 启动/停止的核心脚本，提供了一个通用的框架来管理 Flink 的各种守护进程，根据传入的参数决定要启动或停止
# 哪个守护进程，并且支持同时启动多个实例
# Start/stop a Flink daemon.
USAGE="Usage: flink-daemon.sh (start|stop|stop-all) (taskexecutor|zookeeper|historyserver|standalonesession|standalonejob|sql-gateway) [args]"

# 操作类型参数
STARTSTOP=$1
# 守护进程类型参数
DAEMON=$2
# 从第三个参数起的所有参数
ARGS=("${@:3}") # get remaining arguments as array

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

# 根据守护进程类型参数，映射到对应的 Java 主类，后续会通过这个主类来启动 JVM 实例
case $DAEMON in
    (taskexecutor)
        CLASS_TO_RUN=org.apache.flink.runtime.taskexecutor.TaskManagerRunner
    ;;

    (zookeeper)
        CLASS_TO_RUN=org.apache.flink.runtime.zookeeper.FlinkZooKeeperQuorumPeer
    ;;

    (historyserver)
        CLASS_TO_RUN=org.apache.flink.runtime.webmonitor.history.HistoryServer
    ;;

    (standalonesession)
        CLASS_TO_RUN=org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint
    ;;

    (standalonejob)
        CLASS_TO_RUN=org.apache.flink.container.entrypoint.StandaloneApplicationClusterEntryPoint
    ;;

    # 需要额外调用函数加载相关的 Jar 包路径
    (sql-gateway)
        CLASS_TO_RUN=org.apache.flink.table.gateway.SqlGateway
        SQL_GATEWAY_CLASSPATH="`findSqlGatewayJar`":"`findFlinkPythonJar`"
    ;;

    (*)
        echo "Unknown daemon '${DAEMON}'. $USAGE."
        exit 1
    ;;
esac

# FLINK_IDENT_STRING 用于区分同一台机器上不同用户启动的 Flink 进程，默认使用当前系统用户名，它会出现在 PID 文件
# 名和日志文件名中
if [ "$FLINK_IDENT_STRING" = "" ]; then
    FLINK_IDENT_STRING="$USER"
fi

FLINK_TM_CLASSPATH=`constructFlinkClassPath`

# PID 文件路径
pid=$FLINK_PID_DIR/flink-$FLINK_IDENT_STRING-$DAEMON.pid

mkdir -p "$FLINK_PID_DIR"

# 检测系统是否有 flock 命令
# Log files for daemons are indexed from the process ID's position in the PID
# file. The following lock prevents a race condition during daemon startup
# when multiple daemons read, index, and write to the PID file concurrently.
# The lock is created on the PID directory since a lock file cannot be safely
# removed. The daemon is started with the lock closed and the lock remains
# active in this script until the script exits.
command -v flock >/dev/null 2>&1
if [[ $? -eq 0 ]]; then
    # 用文件描述符 200 打开 PID 目录，并对其加排它锁（默认阻塞等待），防止多个守护进程同时启动时，并发读写 PID 文件
    # 导致日志文件编号冲突，锁在脚本退出时自动释放
    exec 200<"$FLINK_PID_DIR"
    flock 200
fi

# 如果 PID 文件已经存在，则统计其行数，并将行数作为新进程的编号，否则编号为 0，这样同一台机器上可以启动多个相同类型
# 的守护进程，并且它们的日志文件会根据编号区分开来
# Ascending ID depending on number of lines in pid file.
# This allows us to start multiple daemon of each type.
id=$([ -f "$pid" ] && echo $(wc -l < "$pid") || echo "0")

FLINK_LOG_PREFIX="${FLINK_LOG_DIR}/flink-${FLINK_IDENT_STRING}-${DAEMON}-${id}-${HOSTNAME}"
log="${FLINK_LOG_PREFIX}.log"
out="${FLINK_LOG_PREFIX}.out"

# 构建一个数组，包含传给 JVM 的日志配置参数
log_setting=("-Dlog.file=${log}" "-Dlog4j.configuration=file:${FLINK_CONF_DIR}/log4j.properties" "-Dlog4j.configurationFile=file:${FLINK_CONF_DIR}/log4j.properties" "-Dlogback.configurationFile=file:${FLINK_CONF_DIR}/logback.xml")

# 定义一个函数，接收要停止的进程 ID 和守护进程类型作为参数，先发送 SIGTERM 信号请求进程优雅退出，默认在收到信号
# 后 5 秒钟内进程会停止，如果指定了超时时间，则等待指定时间后检查进程是否已经停止，如果没有停止，则发送 SIGKILL
# 信号强制杀死进程，确保进程被完全终止
function guaranteed_kill {
  to_stop_pid=$1
  daemon=$2

  # send sigterm for graceful shutdown
  kill $to_stop_pid
  # if timeout exists, use it
  if command -v timeout &> /dev/null ; then
    # wait 10 seconds for process to stop. By default, Flink kills the JVM 5 seconds after sigterm.
    timeout 10 tail --pid=$to_stop_pid -f /dev/null &> /dev/null
    if [ "$?" -eq 124 ]; then
      echo "Daemon $daemon didn't stop within 10 seconds. Killing it."
      # send sigkill
      kill -9 $to_stop_pid
    fi
  fi
}

# 根据操作类型参数，启动或停止相应的进程
case $STARTSTOP in

    (start)

        # Print a warning if daemons are already running on host
        if [ -f "$pid" ]; then
          active=()
          while IFS='' read -r p || [[ -n "$p" ]]; do
            # kill -0 命令不发送信号，仅检查权限和进程存在性
            kill -0 $p >/dev/null 2>&1
            if [ $? -eq 0 ]; then
              active+=($p)
            fi
          done < "${pid}"

          count="${#active[@]}"

          if [ ${count} -gt 0 ]; then
            echo "[INFO] $count instance(s) of $DAEMON are already running on $HOSTNAME."
          fi
        fi

        # Evaluate user options for local variable expansion
        FLINK_ENV_JAVA_OPTS=$(eval echo ${FLINK_ENV_JAVA_OPTS})

        echo "Starting $DAEMON daemon on host $HOSTNAME."
        # 运行类并传入参数
        "$JAVA_RUN" $JVM_ARGS ${FLINK_ENV_JAVA_OPTS} "${log_setting[@]}" -classpath "`manglePathList "$FLINK_TM_CLASSPATH:$SQL_GATEWAY_CLASSPATH:$INTERNAL_HADOOP_CLASSPATHS"`" ${CLASS_TO_RUN} "${ARGS[@]}" > "$out" 200<&- 2>&1 < /dev/null &

        mypid=$!

        # Add to pid file if successful start
        if [[ ${mypid} =~ ${IS_NUMBER} ]] && kill -0 $mypid > /dev/null 2>&1 ; then
            echo $mypid >> "$pid"
        else
            echo "Error starting $DAEMON daemon."
            exit 1
        fi
    ;;

    (stop)
        if [ -f "$pid" ]; then
            # Remove last in pid file
            to_stop=$(tail -n 1 "$pid")

            if [ -z $to_stop ]; then
                rm "$pid" # If all stopped, clean up pid file
                echo "No $DAEMON daemon to stop on host $HOSTNAME."
            else
                sed \$d "$pid" > "$pid.tmp" # all but last line

                # If all stopped, clean up pid file
                [ $(wc -l < "$pid.tmp") -eq 0 ] && rm "$pid" "$pid.tmp" || mv "$pid.tmp" "$pid"

                if kill -0 $to_stop > /dev/null 2>&1; then
                    echo "Stopping $DAEMON daemon (pid: $to_stop) on host $HOSTNAME."
                    guaranteed_kill $to_stop $DAEMON
                else
                    echo "No $DAEMON daemon (pid: $to_stop) is running anymore on $HOSTNAME."
                fi
            fi
        else
            echo "No $DAEMON daemon to stop on host $HOSTNAME."
        fi
    ;;

    (stop-all)
        if [ -f "$pid" ]; then
            mv "$pid" "${pid}.tmp"

            while read to_stop; do
                if kill -0 $to_stop > /dev/null 2>&1; then
                    echo "Stopping $DAEMON daemon (pid: $to_stop) on host $HOSTNAME."
                    guaranteed_kill $to_stop $DAEMON
                else
                    echo "Skipping $DAEMON daemon (pid: $to_stop), because it is not running anymore on $HOSTNAME."
                fi
            done < "${pid}.tmp"
            rm "${pid}.tmp"
        fi
    ;;

    (*)
        echo "Unexpected argument '$STARTSTOP'. $USAGE."
        exit 1
    ;;

esac
