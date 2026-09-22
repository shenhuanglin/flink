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

package org.apache.flink.kubernetes.cli;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.cli.AbstractCustomCommandLine;
import org.apache.flink.client.cli.CliArgsException;
import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.client.cli.GenericCLI;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.kubernetes.executors.KubernetesSessionClusterExecutor;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClientFactory;
import org.apache.flink.kubernetes.kubeclient.decorators.ExternalServiceDecorator;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.util.FlinkException;

import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import static org.apache.flink.util.Preconditions.checkNotNull;

// KubernetesSession 模式的命令行入口和集群生命周期管理器
/** Kubernetes customized commandline. */
@Internal
public class KubernetesSessionCli {

    private static final Logger LOG = LoggerFactory.getLogger(KubernetesSessionCli.class);

    // ATTACHED 模式下，每次轮询用户输入的最大等待时间，单位毫秒
    private static final long CLIENT_POLLING_INTERVAL_MS = 3000L;

    private static final String KUBERNETES_CLUSTER_HELP =
            "Available commands:\n"
                    + "help - show these commands\n"
                    + "stop - stop the kubernetes cluster\n"
                    + "quit - quit attach mode";

    private final Configuration baseConfiguration;

    // 用于解析命令行参数，将其转换为配置项
    private final GenericCLI cli;
    // SPI 加载器，根据 deployment.target 配置项加载对应的 ClusterClientFactory
    private final ClusterClientServiceLoader clusterClientServiceLoader;

    public KubernetesSessionCli(Configuration configuration, String configDir) {
        this(configuration, new DefaultClusterClientServiceLoader(), configDir);
    }

    public KubernetesSessionCli(
            Configuration configuration,
            ClusterClientServiceLoader clusterClientServiceLoader,
            String configDir) {
        this.baseConfiguration = new UnmodifiableConfiguration(checkNotNull(configuration));
        this.clusterClientServiceLoader = checkNotNull(clusterClientServiceLoader);
        this.cli = new GenericCLI(baseConfiguration, configDir);
    }

    // 获取有效的配置，优先级：命令行参数 > 配置文件 > 默认值
    Configuration getEffectiveConfiguration(String[] args) throws CliArgsException {
        final CommandLine commandLine = cli.parseCommandLineOptions(args, true);
        final Configuration effectiveConfiguration = new Configuration(baseConfiguration);
        // 将命令行参数覆盖到配置中，优先级高于配置文件
        effectiveConfiguration.addAll(cli.toConfiguration(commandLine));
        // 硬编码设置 deployment.target 为 kubernetes-session 模式
        effectiveConfiguration.set(DeploymentOptions.TARGET, KubernetesSessionClusterExecutor.NAME);
        return effectiveConfiguration;
    }

    private int run(String[] args) throws FlinkException, CliArgsException {
        final Configuration configuration = getEffectiveConfiguration(args);

        // 获取 KubernetesClusterClientFactory
        final ClusterClientFactory<String> kubernetesClusterClientFactory =
                clusterClientServiceLoader.getClusterClientFactory(configuration);

        // KubernetesClusterDescriptor 中封装了所有与 Kubernetes 集群交互的动作
        final ClusterDescriptor<String> kubernetesClusterDescriptor =
                kubernetesClusterClientFactory.createClusterDescriptor(configuration);

        // 判断集群是否已经存在
        try {
            final ClusterClient<String> clusterClient;
            String clusterId = kubernetesClusterClientFactory.getClusterId(configuration);
            final boolean detached = !configuration.get(DeploymentOptions.ATTACHED);
            final FlinkKubeClient kubeClient =
                    FlinkKubeClientFactory.getInstance().fromConfiguration(configuration, "client");

            // 如果存在则重新连接，获取 ClusterClient
            // Retrieve or create a session cluster.
            if (clusterId != null
                    && kubeClient
                            .getService(ExternalServiceDecorator.getExternalServiceName(clusterId))
                            .isPresent()) {
                clusterClient = kubernetesClusterDescriptor.retrieve(clusterId).getClusterClient();
            } else {
                clusterClient =
                        kubernetesClusterDescriptor
                                .deploySessionCluster(
                                        kubernetesClusterClientFactory.getClusterSpecification(
                                                configuration))
                                .getClusterClient();
                clusterId = clusterClient.getClusterId();
            }

            // 根据是否是 Detached 模式，决定是否进入交互式命令行界面。Detached 是默认模式，也是生产环境中最常用
            // 的模式，部署完集群后客户端退出，集群长期运行。如果是 Attached 模式，则会进入 REPL 循环，repStep
            // 方法每 3 秒检查一次标准输入，通常在需要交互式管理集群时使用
            try {
                if (!detached) {
                    Tuple2<Boolean, Boolean> continueRepl = new Tuple2<>(true, false);
                    try (BufferedReader in = new BufferedReader(new InputStreamReader(System.in))) {
                        while (continueRepl.f0) {
                            continueRepl = repStep(in);
                        }
                    } catch (Exception e) {
                        LOG.warn(
                                "Exception while running the interactive command line interface.",
                                e);
                    }
                    if (continueRepl.f1) {
                        kubernetesClusterDescriptor.killCluster(clusterId);
                    }
                }
                clusterClient.close();
                kubeClient.close();
            } catch (Exception e) {
                LOG.info("Could not properly shutdown cluster client.", e);
            }
        } finally {
            try {
                kubernetesClusterDescriptor.close();
            } catch (Exception e) {
                LOG.info("Could not properly close the kubernetes cluster descriptor.", e);
            }
        }

        return 0;
    }

    /**
     * Check whether need to continue or kill the cluster.
     *
     * @param in input buffer reader
     * @return f0, whether need to continue read from input. f1, whether need to kill the cluster.
      * 循环读取客户端输入，处理用户的交互式命令
      * @param in 标准输入的 BufferedReader，等待用户输入命令
      * @return f0 代表是否继续读取输入，f1 代表是否需要停止集群
     */
    private Tuple2<Boolean, Boolean> repStep(BufferedReader in)
            throws IOException, InterruptedException {
        final long startTime = System.currentTimeMillis();
        while ((System.currentTimeMillis() - startTime) < CLIENT_POLLING_INTERVAL_MS
                && (!in.ready())) {
            Thread.sleep(200L);
        }
        // ------------- handle interactive command by user. ----------------------

        if (in.ready()) {
            final String command = in.readLine();
            switch (command) {
                case "quit":
                    return new Tuple2<>(false, false);
                case "stop":
                    return new Tuple2<>(false, true);

                case "help":
                    System.err.println(KUBERNETES_CLUSTER_HELP);
                    break;
                default:
                    System.err.println("Unknown command '" + command + "'. Showing help:");
                    System.err.println(KUBERNETES_CLUSTER_HELP);
                    break;
            }
        }

        return new Tuple2<>(true, false);
    }

    public static void main(String[] args) {
        // 加载配置文件
        final Configuration configuration = GlobalConfiguration.loadConfiguration();

        final String configDir = CliFrontend.getConfigurationDirectoryFromEnv();

        int retCode;

        try {
            final KubernetesSessionCli cli = new KubernetesSessionCli(configuration, configDir);
            // 在安全上下文中执行 run 方法
            retCode = SecurityUtils.getInstalledContext().runSecured(() -> cli.run(args));
        } catch (CliArgsException e) {
            retCode = AbstractCustomCommandLine.handleCliArgsException(e, LOG);
        } catch (Exception e) {
            retCode = AbstractCustomCommandLine.handleError(e, LOG);
        }

        System.exit(retCode);
    }
}
// 如果不存在则部署一个新的 Session 集群
