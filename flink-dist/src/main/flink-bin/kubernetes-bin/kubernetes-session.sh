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

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

# get Flink config
. "$bin"/config.sh

# 设置用户表示字符串，区分同一机器上不同用户启动的 Flink 进程
if [ "$FLINK_IDENT_STRING" = "" ]; then
        FLINK_IDENT_STRING="$USER"
fi

# 设置 JVM 参数，限制 KubernetesSessionCli 进程的最大堆内存为 512MB
JVM_ARGS="$JVM_ARGS -Xmx512m"

# 构建类路径，包含 Flink 核心库和 Hadoop 相关的库
CC_CLASSPATH=`manglePathList $(constructFlinkClassPath):$INTERNAL_HADOOP_CLASSPATHS`

# 设置日志文件路径
log=$FLINK_LOG_DIR/flink-$FLINK_IDENT_STRING-k8s-session-$HOSTNAME.log
# 设置日志框架 JVM 参数，指定日志文件和配置文件位置
log_setting="-Dlog.file="$log" -Dlog4j.configuration=file:"$FLINK_CONF_DIR"/log4j-session.properties -Dlog4j.configurationFile=file:"$FLINK_CONF_DIR"/log4j-session.properties -Dlogback.configurationFile=file:"$FLINK_CONF_DIR"/logback-session.xml"

# 将 FLINK_CONF_DIR 导出为环境变量，使其对子进程可见，KubernetesSessionCli 进程中可以读取该环境变量来加载配置文件
export FLINK_CONF_DIR

# 直接启动 KubernetesSessionCli 进程
"$JAVA_RUN" $JVM_ARGS -classpath "$CC_CLASSPATH" $log_setting org.apache.flink.kubernetes.cli.KubernetesSessionCli "$@"
