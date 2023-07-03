/**
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

package org.apache.hadoop.yarn.server.resourcemanager;

import com.google.common.annotations.VisibleForTesting;
import com.sun.jersey.spi.container.servlet.ServletContainer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.apache.curator.framework.AuthInfo;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.curator.ZKCuratorManager;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.ConfigurationProvider;
import org.apache.hadoop.yarn.conf.ConfigurationProviderFactory;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.nodelabels.NodeAttributesManager;
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEventType;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.ApplicationMasterLauncher;
import org.apache.hadoop.yarn.server.resourcemanager.federation.FederationStateStoreService;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.CombinedSystemMetricsPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.NoOpSystemMetricPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.SystemMetricsPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.TimelineServiceV1Publisher;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.TimelineServiceV2Publisher;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NodeAttributesManagerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMDelegatedNodeLabelsUpdater;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.NullRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStoreFactory;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.AbstractReservationSystem;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystem;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceProfilesManager;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceProfilesManagerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.monitor.RMAppLifetimeMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.YarnConfigurationStore;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.YarnConfigurationStoreFactory;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.AllocationTagsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.MemoryPlacementConstraintManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.PlacementConstraintManagerService;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.MultiNodeSortingManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.MutableConfScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.DelegationTokenRenewer;
import org.apache.hadoop.yarn.server.resourcemanager.security.QueueACLsManager;
import org.apache.hadoop.yarn.server.resourcemanager.timelineservice.RMTimelineCollectorManager;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebApp;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebAppUtil;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.service.SystemServiceManager;
import org.apache.hadoop.yarn.server.webproxy.AppReportFetcher;
import org.apache.hadoop.yarn.server.webproxy.ProxyUriUtils;
import org.apache.hadoop.yarn.server.webproxy.WebAppProxy;
import org.apache.hadoop.yarn.server.webproxy.WebAppProxyServlet;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.apache.hadoop.yarn.webapp.WebApps.Builder;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.eclipse.jetty.webapp.WebAppContext;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.Charset;
import java.security.PrivilegedExceptionAction;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * The ResourceManager is the main class that is a set of components.
 * "I am the ResourceManager. All your resources belong to us..."
 *
 */
@SuppressWarnings("unchecked")
public class ResourceManager extends CompositeService
        implements Recoverable, ResourceManagerMXBean {

  /**
   * Priority of the ResourceManager shutdown hook.
   */
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;

  /**
   * Used for generation of various ids.
   */
  public static final int EPOCH_BIT_SHIFT = 40;

  private static final Logger LOG =
      LoggerFactory.getLogger(ResourceManager.class);
  private static final Marker FATAL = MarkerFactory.getMarker("FATAL");
  private static long clusterTimeStamp = System.currentTimeMillis();

  /*
   * UI2 webapp name
   */
  public static final String UI2_WEBAPP_NAME = "/ui2";

  /**
   * "Always On" services. Services that need to run always irrespective of
   * the HA state of the RM.
   */
  @VisibleForTesting
  protected RMContextImpl rmContext;
  private Dispatcher rmDispatcher;
  @VisibleForTesting
  protected AdminService adminService;

  /**
   * "Active" services. Services that need to run only on the Active RM.
   * These services are managed (initialized, started, stopped) by the
   * {@link CompositeService} RMActiveServices.
   *
   * RM is active when (1) HA is disabled, or (2) HA is enabled and the RM is
   * in Active state.
   */
  protected RMActiveServices activeServices;
  protected RMSecretManagerService rmSecretManagerService;

  protected ResourceScheduler scheduler;
  protected ReservationSystem reservationSystem;
  private ClientRMService clientRM;
  protected ApplicationMasterService masterService;
  protected NMLivelinessMonitor nmLivelinessMonitor;
  protected NodesListManager nodesListManager;
  protected RMAppManager rmAppManager;
  protected ApplicationACLsManager applicationACLsManager;
  protected QueueACLsManager queueACLsManager;
  private FederationStateStoreService federationStateStoreService;
  private WebApp webApp;
  private AppReportFetcher fetcher = null;
  protected ResourceTrackerService resourceTracker;
  private JvmMetrics jvmMetrics;
  private boolean curatorEnabled = false;
  private ZKCuratorManager zkManager;
  private final String zkRootNodePassword =
      Long.toString(new SecureRandom().nextLong());
  private boolean recoveryEnabled;

  @VisibleForTesting
  protected String webAppAddress;
  private ConfigurationProvider configurationProvider = null;
  /** End of Active services */

  private Configuration conf;

  private UserGroupInformation rmLoginUGI;

  public ResourceManager() {
    super("ResourceManager");
  }

  public RMContext getRMContext() {
    return this.rmContext;
  }

  public static long getClusterTimeStamp() {
    return clusterTimeStamp;
  }

  public String getRMLoginUser() {
    return rmLoginUGI.getShortUserName();
  }

  @VisibleForTesting
  protected static void setClusterTimeStamp(long timestamp) {
    clusterTimeStamp = timestamp;
  }

  @VisibleForTesting
  Dispatcher getRmDispatcher() {
    return rmDispatcher;
  }

  @VisibleForTesting
  protected ResourceProfilesManager createResourceProfileManager() {
    ResourceProfilesManager resourceProfilesManager =
        new ResourceProfilesManagerImpl();
    return resourceProfilesManager;
  }

  /*************************************************
   * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
   *  注释：
   *  1、创建各种子service实例
   *      new XXXX(); createXXX();
   *  2、将该实例加入到 serviceList 集合中
   *      addService(service);  addIfService(object)
   *  3、执行各个子service 的 serviceInit() 方法
   *      super.init();
   */
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    this.conf = conf;
    UserGroupInformation.setConfiguration(conf);
    //todo
    // TODO_MA 注释： 初始化 ResourceManager Context
    // TODO_MA 注释： 里面会存储很多重要成员，在这之后会创建出来，然后设置到 RMContextImpl
    this.rmContext = new RMContextImpl();
    // TODO 注释： ResourceManager 是 RMContext 的成员变量
    rmContext.setResourceManager(this);
    // TODO 注释： LocalConfigurationProvider
    this.configurationProvider =
        ConfigurationProviderFactory.getConfigurationProvider(conf);
    this.configurationProvider.init(this.conf);
    rmContext.setConfigurationProvider(configurationProvider);

    //todo 注释： 读取 core-site.xml 配置
    // load core-site.xml
    loadConfigurationXml(YarnConfiguration.CORE_SITE_CONFIGURATION_FILE);

    // Do refreshSuperUserGroupsConfiguration with loaded core-site.xml
    // Or use RM specific configurations to overwrite the common ones first
    // if they exist
    RMServerUtils.processRMProxyUsersConf(conf);
    ProxyUsers.refreshSuperUserGroupsConfiguration(this.conf);
    //todo 注释： 读取 yarn-site.xml 配置
    // load yarn-site.xml
    loadConfigurationXml(YarnConfiguration.YARN_SITE_CONFIGURATION_FILE);

    validateConfigs(this.conf);

    // TODO 注释： yarn.resourcemanager.ha.enabled = true
    // TODO 注释： HA 的一些设置 和 相关服务地址的检查
    // Set HA configuration should be done before login
    this.rmContext.setHAEnabled(HAUtil.isHAEnabled(this.conf));
    if (this.rmContext.isHAEnabled()) {
      HAUtil.verifyAndSetConfiguration(this.conf);
    }

    // Set UGI and do login
    // If security is enabled, use login user
    // If security is not enabled, use current user
    this.rmLoginUGI = UserGroupInformation.getCurrentUser();
    try {
      doSecureLogin();
    } catch(IOException ie) {
      throw new YarnRuntimeException("Failed to login", ie);
    }
    //todo 注释： 初始化和添加 AsyncDispatcher
    // register the handlers for all AlwaysOn services using setupDispatcher().
    rmDispatcher = setupDispatcher();
    addIfService(rmDispatcher);
    rmContext.setDispatcher(rmDispatcher);

    // The order of services below should not be changed as services will be
    // started in same order
    // As elector service needs admin service to be initialized and started,
    // first we add admin service then elector service

    //* todo 注释： 初始化和添加 AdminService
    //         *  AdminService 为管理员提供了一套独立的服务接口，以防止大量的普通用户的请求使得管理员发送的管理命令饿死。
    //         *  管理员可以通过这些接口命令管理集群，比如动态更新节点列表，更新ACL列表，更新队列信息等
    //         */
    adminService = createAdminService();
    addService(adminService);
    rmContext.setRMAdminService(adminService);

    // elector must be added post adminservice
    //todo 注释： 选举服务
    if (this.rmContext.isHAEnabled()) {
      // TODO 注释： 默认这两个条件，都是 true
      // TODO 注释： yarn.resourcemanager.ha.automatic-failover.enabled = true
      // TODO 注释： yarn.resourcemanager.ha.automatic-failover.embedded = true
      // TODO 注释： 如果上一个参数为 true 表示使用原生 zookeeper API 来执行选举
      // TODO 注释： 否则使用： curator 框架来实现！
      // If the RM is configured to use an embedded leader elector,
      // initialize the leader elector.
      if (HAUtil.isAutomaticFailoverEnabled(conf)
          && HAUtil.isAutomaticFailoverEmbedded(conf)) {
        // TODO 注释： 创建选举器 ActiveStandbyElectorBasedElectorService
        EmbeddedElector elector = createEmbeddedElector();
        addIfService(elector);
        rmContext.setLeaderElectorService(elector);
      }
    }

    rmContext.setYarnConfiguration(conf);
    /**********************【重点！！！】***************************
     * TODO 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 里面创建了很多 Service 实例，都加入到了 serviceList 集合中
     *  内部创建了一个： StandByTransitionRunnable 选举线程
     *  ActiveService 是只有 Active RM 才会启动的 服务 service
     */
    createAndInitActiveServices(false);
    //todo  注释： WebApp 默认端口： 8090
    webAppAddress = WebAppUtils.getWebAppBindURL(this.conf,
                      YarnConfiguration.RM_BIND_HOST,
                      WebAppUtils.getRMWebAppURLWithoutScheme(this.conf));
    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 创建 RMApplicationHistoryWriter，然后加入 serviceList
     *  持久化 RMApp, RMAppAttempt, RMContainer 的信息
     */
    RMApplicationHistoryWriter rmApplicationHistoryWriter =
        createRMApplicationHistoryWriter();
    addService(rmApplicationHistoryWriter);
    rmContext.setRMApplicationHistoryWriter(rmApplicationHistoryWriter);

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 创建 RMTimelineCollectorManager，然后加入 serviceList
     *  默认没有开启
     */
    // initialize the RM timeline collector first so that the system metrics
    // publisher can bind to it
    if (YarnConfiguration.timelineServiceV2Enabled(this.conf)) {
      RMTimelineCollectorManager timelineCollectorManager =
          createRMTimelineCollectorManager();
      addService(timelineCollectorManager);
      rmContext.setRMTimelineCollectorManager(timelineCollectorManager);
    }

    SystemMetricsPublisher systemMetricsPublisher =
        createSystemMetricsPublisher();
    addIfService(systemMetricsPublisher);
    rmContext.setSystemMetricsPublisher(systemMetricsPublisher);

    registerMXBean();

    super.serviceInit(this.conf);
  }

  private void loadConfigurationXml(String configurationFile)
      throws YarnException, IOException {
    InputStream configurationInputStream =
        this.configurationProvider.getConfigurationInputStream(this.conf,
            configurationFile);
    if (configurationInputStream != null) {
      this.conf.addResource(configurationInputStream, configurationFile);
    }
  }

  protected EmbeddedElector createEmbeddedElector() throws IOException {
    EmbeddedElector elector;
    // TODO 注释： 基于 Curator 实现， 默认是 false
    curatorEnabled =
        conf.getBoolean(YarnConfiguration.CURATOR_LEADER_ELECTOR,
            YarnConfiguration.DEFAULT_CURATOR_LEADER_ELECTOR_ENABLED);
    if (curatorEnabled) {
      this.zkManager = createAndStartZKManager(conf);
      elector = new CuratorBasedElectorService(this);
    } else {
      //todo 基于zk原生api
      /*************************************************
       * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
       *  注释： 内部封装了 ActiveStandbyElector 实例
       *  任何一个service的源码阅读分为三个部分：
       *  1、实例对象的创建
       *  2、serviceInit()
       *  3、serviceStart()
       */
      elector = new ActiveStandbyElectorBasedElectorService(this);
    }
    return elector;
  }

  /**
   * Get ZooKeeper Curator manager, creating and starting if not exists.
   * @param config Configuration for the ZooKeeper curator.
   * @return ZooKeeper Curator manager.
   * @throws IOException If it cannot create the manager.
   */
  public ZKCuratorManager createAndStartZKManager(Configuration
      config) throws IOException {
    ZKCuratorManager manager = new ZKCuratorManager(config);

    // Get authentication
    List<AuthInfo> authInfos = new ArrayList<>();
    if (HAUtil.isHAEnabled(config) && HAUtil.getConfValueForRMInstance(
        YarnConfiguration.ZK_RM_STATE_STORE_ROOT_NODE_ACL, config) == null) {
      String zkRootNodeUsername = HAUtil.getConfValueForRMInstance(
          YarnConfiguration.RM_ADDRESS,
          YarnConfiguration.DEFAULT_RM_ADDRESS, config);
      String defaultFencingAuth =
          zkRootNodeUsername + ":" + zkRootNodePassword;
      byte[] defaultFencingAuthData =
          defaultFencingAuth.getBytes(Charset.forName("UTF-8"));
      String scheme = new DigestAuthenticationProvider().getScheme();
      AuthInfo authInfo = new AuthInfo(scheme, defaultFencingAuthData);
      authInfos.add(authInfo);
    }

    manager.start(authInfos);
    return manager;
  }

  public ZKCuratorManager getZKManager() {
    return zkManager;
  }

  public CuratorFramework getCurator() {
    if (this.zkManager == null) {
      return null;
    }
    return this.zkManager.getCurator();
  }

  public String getZkRootNodePassword() {
    return this.zkRootNodePassword;
  }


  protected QueueACLsManager createQueueACLsManager(ResourceScheduler scheduler,
      Configuration conf) {
    return new QueueACLsManager(scheduler, conf);
  }

  @VisibleForTesting
  protected void setRMStateStore(RMStateStore rmStore) {
    rmStore.setRMDispatcher(rmDispatcher);
    rmStore.setResourceManager(this);
    rmContext.setStateStore(rmStore);
  }

  protected EventHandler<SchedulerEvent> createSchedulerEventDispatcher() {
    return new EventDispatcher(this.scheduler, "SchedulerEventDispatcher");
  }

  protected Dispatcher createDispatcher() {
    return new AsyncDispatcher("RM Event dispatcher");
  }

  protected ResourceScheduler createScheduler() {
    /*************************************************
     * TODO 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 默认调度器：
     *  key = yarn.resourcemanager.scheduler.class
     *  value = org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler
     */
    String schedulerClassName = conf.get(YarnConfiguration.RM_SCHEDULER,
        YarnConfiguration.DEFAULT_RM_SCHEDULER);
    LOG.info("Using Scheduler: " + schedulerClassName);
    // TODO 注释： 通过反射构建 CapacityScheduler 实例对象
    try {
      Class<?> schedulerClazz = Class.forName(schedulerClassName);
      if (ResourceScheduler.class.isAssignableFrom(schedulerClazz)) {
        return (ResourceScheduler) ReflectionUtils.newInstance(schedulerClazz,
            this.conf);
      } else {
        throw new YarnRuntimeException("Class: " + schedulerClassName
            + " not instance of " + ResourceScheduler.class.getCanonicalName());
      }
    } catch (ClassNotFoundException e) {
      throw new YarnRuntimeException("Could not instantiate Scheduler: "
          + schedulerClassName, e);
    }
  }

  protected ReservationSystem createReservationSystem() {
    String reservationClassName =
        conf.get(YarnConfiguration.RM_RESERVATION_SYSTEM_CLASS,
            AbstractReservationSystem.getDefaultReservationSystem(scheduler));
    if (reservationClassName == null) {
      return null;
    }
    LOG.info("Using ReservationSystem: " + reservationClassName);
    try {
      Class<?> reservationClazz = Class.forName(reservationClassName);
      if (ReservationSystem.class.isAssignableFrom(reservationClazz)) {
        return (ReservationSystem) ReflectionUtils.newInstance(
            reservationClazz, this.conf);
      } else {
        throw new YarnRuntimeException("Class: " + reservationClassName
            + " not instance of " + ReservationSystem.class.getCanonicalName());
      }
    } catch (ClassNotFoundException e) {
      throw new YarnRuntimeException(
          "Could not instantiate ReservationSystem: " + reservationClassName, e);
    }
  }

  protected SystemServiceManager createServiceManager() {
    String schedulerClassName =
        YarnConfiguration.DEFAULT_YARN_API_SYSTEM_SERVICES_CLASS;
    LOG.info("Using SystemServiceManager: " + schedulerClassName);
    try {
      Class<?> schedulerClazz = Class.forName(schedulerClassName);
      if (SystemServiceManager.class.isAssignableFrom(schedulerClazz)) {
        return (SystemServiceManager) ReflectionUtils
            .newInstance(schedulerClazz, this.conf);
      } else {
        throw new YarnRuntimeException(
            "Class: " + schedulerClassName + " not instance of "
                + SystemServiceManager.class.getCanonicalName());
      }
    } catch (ClassNotFoundException e) {
      throw new YarnRuntimeException(
          "Could not instantiate SystemServiceManager: " + schedulerClassName,
          e);
    }
  }

  protected ApplicationMasterLauncher createAMLauncher() {
    return new ApplicationMasterLauncher(this.rmContext);
  }

  private NMLivelinessMonitor createNMLivelinessMonitor() {
    return new NMLivelinessMonitor(this.rmContext
        .getDispatcher());
  }

  protected AMLivelinessMonitor createAMLivelinessMonitor() {
    return new AMLivelinessMonitor(this.rmDispatcher);
  }
  
  protected RMNodeLabelsManager createNodeLabelManager()
      throws InstantiationException, IllegalAccessException {
    return new RMNodeLabelsManager();
  }

  protected NodeAttributesManager createNodeAttributesManager() {
    NodeAttributesManagerImpl namImpl = new NodeAttributesManagerImpl();
    namImpl.setRMContext(rmContext);
    return namImpl;
  }

  protected AllocationTagsManager createAllocationTagsManager() {
    return new AllocationTagsManager(this.rmContext);
  }

  protected PlacementConstraintManagerService
      createPlacementConstraintManager() {
    // Use the in memory Placement Constraint Manager.
    return new MemoryPlacementConstraintManager();
  }
  
  protected DelegationTokenRenewer createDelegationTokenRenewer() {
    return new DelegationTokenRenewer();
  }

  protected RMAppManager createRMAppManager() {
    return new RMAppManager(this.rmContext, this.scheduler, this.masterService,
      this.applicationACLsManager, this.conf);
  }

  protected RMApplicationHistoryWriter createRMApplicationHistoryWriter() {
    return new RMApplicationHistoryWriter();
  }

  private RMTimelineCollectorManager createRMTimelineCollectorManager() {
    return new RMTimelineCollectorManager(this);
  }

  private FederationStateStoreService createFederationStateStoreService() {
    return new FederationStateStoreService(rmContext);
  }

  protected MultiNodeSortingManager<SchedulerNode> createMultiNodeSortingManager() {
    return new MultiNodeSortingManager<SchedulerNode>();
  }

  protected SystemMetricsPublisher createSystemMetricsPublisher() {
    List<SystemMetricsPublisher> publishers =
        new ArrayList<SystemMetricsPublisher>();
    if (YarnConfiguration.timelineServiceV1Enabled(conf)) {
      SystemMetricsPublisher publisherV1 = new TimelineServiceV1Publisher();
      publishers.add(publisherV1);
    }
    if (YarnConfiguration.timelineServiceV2Enabled(conf)) {
      // we're dealing with the v.2.x publisher
      LOG.info("system metrics publisher with the timeline service V2 is "
          + "configured");
      SystemMetricsPublisher publisherV2 = new TimelineServiceV2Publisher(
          rmContext.getRMTimelineCollectorManager());
      publishers.add(publisherV2);
    }
    if (publishers.isEmpty()) {
      LOG.info("TimelineServicePublisher is not configured");
      SystemMetricsPublisher noopPublisher = new NoOpSystemMetricPublisher();
      publishers.add(noopPublisher);
    }

    for (SystemMetricsPublisher publisher : publishers) {
      addIfService(publisher);
    }

    SystemMetricsPublisher combinedPublisher =
        new CombinedSystemMetricsPublisher(publishers);
    return combinedPublisher;
  }

  // sanity check for configurations
  protected static void validateConfigs(Configuration conf) {
    // validate max-attempts
    int rmMaxAppAttempts = conf.getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
        YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
    if (rmMaxAppAttempts <= 0) {
      throw new YarnRuntimeException("Invalid rm am max attempts configuration"
          + ", " + YarnConfiguration.RM_AM_MAX_ATTEMPTS
          + "=" + rmMaxAppAttempts + ", it should be a positive integer.");
    }
    int globalMaxAppAttempts = conf.getInt(
        YarnConfiguration.GLOBAL_RM_AM_MAX_ATTEMPTS,
        conf.getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
            YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS));
    if (globalMaxAppAttempts <= 0) {
      throw new YarnRuntimeException("Invalid global max attempts configuration"
          + ", " + YarnConfiguration.GLOBAL_RM_AM_MAX_ATTEMPTS
          + "=" + globalMaxAppAttempts + ", it should be a positive integer.");
    }

    // validate expireIntvl >= heartbeatIntvl
    long expireIntvl = conf.getLong(YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS,
        YarnConfiguration.DEFAULT_RM_NM_EXPIRY_INTERVAL_MS);
    long heartbeatIntvl =
        conf.getLong(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS,
            YarnConfiguration.DEFAULT_RM_NM_HEARTBEAT_INTERVAL_MS);
    if (expireIntvl < heartbeatIntvl) {
      throw new YarnRuntimeException("Nodemanager expiry interval should be no"
          + " less than heartbeat interval, "
          + YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS + "=" + expireIntvl
          + ", " + YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS + "="
          + heartbeatIntvl);
    }
  }

  /**
   * RMActiveServices handles all the Active services in the RM.
   */
  @Private
  public class RMActiveServices extends CompositeService {

    private DelegationTokenRenewer delegationTokenRenewer;
    // TODO 注释： 调度器对应的 EventHandler
    private EventHandler<SchedulerEvent> schedulerDispatcher;
    // TODO 注释： ApplicationMaster 启动器
    private ApplicationMasterLauncher applicationMasterLauncher;
    // TODO 注释： Container 过期检测器
    private ContainerAllocationExpirer containerAllocationExpirer;
    private ResourceManager rm;
    private boolean fromActive = false;
    private StandByTransitionRunnable standByTransitionRunnable;
    private RMNMInfo rmnmInfo;

    RMActiveServices(ResourceManager rm) {
      super("RMActiveServices");
      this.rm = rm;
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 这个 RMActiveServices 是继承自 CompositeService，那么他应该也是组合了多个 Service
     *  RMActiveServices 处理所有 RM 中的活跃的(Active)服务
     */
    @Override
    protected void serviceInit(Configuration configuration) throws Exception {
      /*************************************************
       * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
       *  注释： 选举线程
       *  1、之前创建了一个 选举实例
       *  2、此时创建了一个 选举线程
       *  选举实例，会尝试最多 3 次选举，如果没有成功，则启动这个线程来执行选举
       */
      standByTransitionRunnable = new StandByTransitionRunnable();
      /*************************************************
       * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
       *  注释： RMSecretManagerService
       *  RMSecretManagerService 主要提供了一些Token相关服务
       */
      rmSecretManagerService = createRMSecretManagerService();
      addService(rmSecretManagerService);
      /*************************************************
       * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
       *  注释： 监控 Container 是否过期（提交 ApplicationMaster 时检查）
       *  当 AM 收到 RM 新分配的一个 Container 后，必须在一定的时间（默认为 10min）
       *  内在对应的 NM 上启动该 Container，否则，RM 会回收该 Container。
       */
      containerAllocationExpirer = new ContainerAllocationExpirer(rmDispatcher);
      addService(containerAllocationExpirer);
      rmContext.setContainerAllocationExpirer(containerAllocationExpirer);
      /*************************************************
       * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
       *  注释： 监听 ApplicationMaster 状态 = AMLivelinessMonitor
       *  AM 存活监控，继承自 AbstractLivelinessMonitor，过期发生时会触发回调函数
       */
      AMLivelinessMonitor amLivelinessMonitor = createAMLivelinessMonitor();
      addService(amLivelinessMonitor);
      rmContext.setAMLivelinessMonitor(amLivelinessMonitor);
      /*************************************************
       * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
       *  注释： AM 结束监控
       */
      AMLivelinessMonitor amFinishingMonitor = createAMLivelinessMonitor();
      addService(amFinishingMonitor);
      rmContext.setAMFinishingMonitor(amFinishingMonitor);
      /*************************************************
       * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
       *  注释： RMApp 生命周期管理
       *  如果一个 RMApp 在给定时间里面，没有运行完毕，则会被关闭掉
       */
      RMAppLifetimeMonitor rmAppLifetimeMonitor = createRMAppLifetimeMonitor();
      addService(rmAppLifetimeMonitor);
      rmContext.setRMAppLifetimeMonitor(rmAppLifetimeMonitor);
      /*************************************************
       * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
       *  注释： RMNode 标签管理者
       */
      RMNodeLabelsManager nlm = createNodeLabelManager();
      nlm.setRMContext(rmContext);
      addService(nlm);
      rmContext.setNodeLabelManager(nlm);
      /*************************************************
       * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
       *  注释： NodeManager 属性管理
       */
      NodeAttributesManager nam = createNodeAttributesManager();
      addService(nam);
      rmContext.setNodeAttributesManager(nam);
      /*************************************************
       * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
       *  注释： AllocationTags 管理器
       */
      AllocationTagsManager allocationTagsManager =
          createAllocationTagsManager();
      rmContext.setAllocationTagsManager(allocationTagsManager);
      /*************************************************
       * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
       *  注释： MemoryPlacementConstraintManager
       */
      PlacementConstraintManagerService placementConstraintManager =
          createPlacementConstraintManager();
      addService(placementConstraintManager);
      rmContext.setPlacementConstraintManager(placementConstraintManager);
      /*************************************************
       * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
       *  注释： ResourceProfilesManagerImpl
       */
      // add resource profiles here because it's used by AbstractYarnScheduler
      ResourceProfilesManager resourceProfilesManager =
          createResourceProfileManager();
      resourceProfilesManager.init(conf);
      rmContext.setResourceProfilesManager(resourceProfilesManager);
      /*************************************************
       * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
       *  注释： MultiNodeSortingManager
       */
      MultiNodeSortingManager<SchedulerNode> multiNodeSortingManager =
          createMultiNodeSortingManager();
      multiNodeSortingManager.setRMContext(rmContext);
      addService(multiNodeSortingManager);
      rmContext.setMultiNodeSortingManager(multiNodeSortingManager);
      /*************************************************
       * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
       *  注释： 默认false，则 delegatedNodeLabelsUpdater = null;
       *  默认： RMDelegatedNodeLabelsUpdater
       */
      RMDelegatedNodeLabelsUpdater delegatedNodeLabelsUpdater =
          createRMDelegatedNodeLabelsUpdater();
      if (delegatedNodeLabelsUpdater != null) {
        addService(delegatedNodeLabelsUpdater);
        rmContext.setRMDelegatedNodeLabelsUpdater(delegatedNodeLabelsUpdater);
      }
      // TODO 注释： yarn.resourcemanager.recovery.enabled = false
      recoveryEnabled = conf.getBoolean(YarnConfiguration.RECOVERY_ENABLED,
          YarnConfiguration.DEFAULT_RM_RECOVERY_ENABLED);
      /*************************************************
       * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
       *  注释： 如果启用了 recoveryEnabled， 则 rmStore 默认实现是 MemoryRMStateStore
       *  管理 RM 状态的存储，我们用的是 MemoryRMStateStore
       *  可以通过 yarn.resourcemanager.store.class 配置修改
       */
      RMStateStore rmStore = null;
      if (recoveryEnabled) {
        rmStore = RMStateStoreFactory.getStore(conf);
        boolean isWorkPreservingRecoveryEnabled =
            conf.getBoolean(
              YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_ENABLED,
              YarnConfiguration.DEFAULT_RM_WORK_PRESERVING_RECOVERY_ENABLED);
        rmContext
            .setWorkPreservingRecoveryEnabled(isWorkPreservingRecoveryEnabled);
      } else {
        rmStore = new NullRMStateStore();
      }

      try {
        rmStore.setResourceManager(rm);
        rmStore.init(conf);
        rmStore.setRMDispatcher(rmDispatcher);
      } catch (Exception e) {
        // the Exception from stateStore.init() needs to be handled for
        // HA and we need to give up master status if we got fenced
        LOG.error("Failed to init state store", e);
        throw e;
      }
      rmContext.setStateStore(rmStore);

      if (UserGroupInformation.isSecurityEnabled()) {
        delegationTokenRenewer = createDelegationTokenRenewer();
        rmContext.setDelegationTokenRenewer(delegationTokenRenewer);
      }
      /*************************************************
       * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
       *  注释： Node 列表管理器，还用 rmDispatcher 注册了一个 NodesListManagerEventType 事件处理(节点可用\不可用)
       */
      // Register event handler for NodesListManager
      nodesListManager = new NodesListManager(rmContext);
      rmDispatcher.register(NodesListManagerEventType.class, nodesListManager);
      addService(nodesListManager);
      rmContext.setNodesListManager(nodesListManager);
      /*************************************************
       * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
       *  注释： 默认调度器： org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler
       *  ResourceScheduler 调度器的创建, 他的子类之一就是 CapacityScheduler
       */
      // Initialize the scheduler
      scheduler = createScheduler();
      scheduler.setRMContext(rmContext);
      addIfService(scheduler);
      rmContext.setScheduler(scheduler);
      /*************************************************
       * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
       *  注释： 返回 EventDispatcher
       */
      schedulerDispatcher = createSchedulerEventDispatcher();
      addIfService(schedulerDispatcher);
      // TODO_MA 注释： 用 rmDispatcher 注册了一个 SchedulerEventType 事件处理
      // TODO_MA 注释： rmDispatcher = AsyncDispatcher
      // TODO_MA 注释： 注册方法有两个参数：
      // TODO_MA 注释： 第一个参数： 事件类型
      // TODO_MA 注释： 第二个参数： 事件处理器
      // TODO_MA 注释： 如果提交一个事件到 AsyncDispatcher 那么这个 AsyncDispatcher 就会找到之前注册的这个事件对应的
      // TODO_MA 注释： EventHandler 来执行事件的处理
      // TODO_MA 注释： EventHandler 有可能是一个状态机，也有可能是一个普通的 EventHandler
      // SchedulerEventType ==> CapacityScheduler
      rmDispatcher.register(SchedulerEventType.class, schedulerDispatcher);

      // Register event handler for RmAppEvents
      rmDispatcher.register(RMAppEventType.class,
          new ApplicationEventDispatcher(rmContext));

      // Register event handler for RmAppAttemptEvents
      rmDispatcher.register(RMAppAttemptEventType.class,
          new ApplicationAttemptEventDispatcher(rmContext));

      // Register event handler for RmNodes
      rmDispatcher.register(
          RMNodeEventType.class, new NodeEventDispatcher(rmContext));
      /*************************************************
       * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
       *  注释： NM 存活监控
       */
      nmLivelinessMonitor = createNMLivelinessMonitor();
      addService(nmLivelinessMonitor);
      /*************************************************
       * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
       *  注释： 创建资源管理服务。
       *  处理来自 NodeManager 的请求，主要包括两种请求：注册和心跳.
       *  其中，注册是 NodeManager 启动时发生的行为，请求包中包含节点 ID，可用的资源上限等信息;
       *  而心跳是周期性行为，包含各个 Container 运行状态，运行的 Application 列表、节点健康状况（可通过一个脚本设置），
       *  以上请求调用通过 hadoop 自己实现的一套 RPC 协议实现，具体看看 YarnRPC。
       */
      resourceTracker = createResourceTrackerService();
      addService(resourceTracker);
      rmContext.setResourceTrackerService(resourceTracker);
      // TODO 注释： 监控 jvm 运行状况，异常就记录日志
      MetricsSystem ms = DefaultMetricsSystem.initialize("ResourceManager");
      if (fromActive) {
        JvmMetrics.reattach(ms, jvmMetrics);
        UserGroupInformation.reattachMetrics();
      } else {
        jvmMetrics = JvmMetrics.initSingleton("ResourceManager", null);
      }
      /*************************************************
       * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
       *  注释： Jvm 暂停监视器
       */
      JvmPauseMonitor pauseMonitor = new JvmPauseMonitor();
      addService(pauseMonitor);
      jvmMetrics.setPauseMonitor(pauseMonitor);

      // Initialize the Reservation system
      if (conf.getBoolean(YarnConfiguration.RM_RESERVATION_SYSTEM_ENABLE,
          YarnConfiguration.DEFAULT_RM_RESERVATION_SYSTEM_ENABLE)) {
        reservationSystem = createReservationSystem();
        if (reservationSystem != null) {
          reservationSystem.setRMContext(rmContext);
          addIfService(reservationSystem);
          rmContext.setReservationSystem(reservationSystem);
          LOG.info("Initialized Reservation system");
        }
      }
      /*************************************************
       * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
       *  注释： 用于对所有提交的 ApplicationMaster 进行管理。
       *  该组件响应所有来自AM的请求，实现了 ApplicationMasterProtocol 协议，这个协议是 AM 与 RM 通信的唯一协议。
       *  主要包括以下任务：
       *  1、注册新的 AM、来自任意正在结束的 AM 的终止/取消注册请求、认证来自不同 AM 的所有请求，
       *  2、确保合法的 AM 发送的请求传递给 RM 中的应用程序对象、获取来自所有运行 AM 的 Container 的分配和释放请求、异步的转发给 Yarn 调度器。
       *  3、ApplicaitonMaster Service 确保了任意时间点、任意 AM 只有一个线程可以发送请求给 RM，因为在 RM 上所有来自 AM 的 RPC 请求都串行化了。
       */
      masterService = createApplicationMasterService();
      createAndRegisterOpportunisticDispatcher(masterService);
      addService(masterService) ;
      rmContext.setApplicationMasterService(masterService);

      /*************************************************
       * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
       *  注释： 内部封装： AdminACLsManager
       */
      applicationACLsManager = new ApplicationACLsManager(conf);
      /*************************************************
       * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
       *  注释： QueueACLsManager
       */
      queueACLsManager = createQueueACLsManager(scheduler, conf);
      /*************************************************
       * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
       *  注释： 维护 applications list，管理 app 提交、结束、恢复等
       */
      rmAppManager = createRMAppManager();
      // Register event handler for RMAppManagerEvents
      rmDispatcher.register(RMAppManagerEventType.class, rmAppManager);
      /*************************************************
       * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
       *  注释： ClientRMService
       *  负责处理面向客户端使用的接口，内部实现了 Client 和 RM 之间通讯的 ApplicationClientProtocol 协议
       */
      clientRM = createClientRMService();
      addService(clientRM);
      rmContext.setClientRMService(clientRM);
      /*************************************************
       * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
       *  注释： 负责启动和停止 ApplicationMaster
       *  applicationMasterLauncher = ApplicationMasterLauncher
       */
      applicationMasterLauncher = createAMLauncher();
      rmDispatcher.register(AMLauncherEventType.class,
          applicationMasterLauncher);

      addService(applicationMasterLauncher);
      if (UserGroupInformation.isSecurityEnabled()) {
        addService(delegationTokenRenewer);
        delegationTokenRenewer.setRMContext(rmContext);
      }

      if(HAUtil.isFederationEnabled(conf)) {
        String cId = YarnConfiguration.getClusterId(conf);
        if (cId.isEmpty()) {
          String errMsg =
              "Cannot initialize RM as Federation is enabled"
                  + " but cluster id is not configured.";
          LOG.error(errMsg);
          throw new YarnRuntimeException(errMsg);
        }
        federationStateStoreService = createFederationStateStoreService();
        addIfService(federationStateStoreService);
        LOG.info("Initialized Federation membership.");
      }

      rmnmInfo = new RMNMInfo(rmContext, scheduler);
      /*************************************************
       * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
       *  注释： 默认实现： SystemServiceManagerImpl
       */
      if (conf.getBoolean(YarnConfiguration.YARN_API_SERVICES_ENABLE,
          false)) {
        SystemServiceManager systemServiceManager = createServiceManager();
        addIfService(systemServiceManager);
      }

      super.serviceInit(conf);
    }

    private void createAndRegisterOpportunisticDispatcher(
        ApplicationMasterService service) {
      if (!isOpportunisticSchedulingEnabled(conf)) {
        return;
      }
      EventDispatcher oppContainerAllocEventDispatcher = new EventDispatcher(
          (OpportunisticContainerAllocatorAMService) service,
          OpportunisticContainerAllocatorAMService.class.getName());
      // Add an event dispatcher for the
      // OpportunisticContainerAllocatorAMService to handle node
      // additions, updates and removals. Since the SchedulerEvent is currently
      // a super set of theses, we register interest for it.
      addService(oppContainerAllocEventDispatcher);
      rmDispatcher
          .register(SchedulerEventType.class, oppContainerAllocEventDispatcher);
    }

    @Override
    protected void serviceStart() throws Exception {
      RMStateStore rmStore = rmContext.getStateStore();
      // The state store needs to start irrespective of recoveryEnabled as apps
      // need events to move to further states.
      rmStore.start();

      if(recoveryEnabled) {
        try {
          LOG.info("Recovery started");
          rmStore.checkVersion();
          if (rmContext.isWorkPreservingRecoveryEnabled()) {
            rmContext.setEpoch(rmStore.getAndIncrementEpoch());
          }
          RMState state = rmStore.loadState();
          recover(state);
          LOG.info("Recovery ended");
        } catch (Exception e) {
          // the Exception from loadState() needs to be handled for
          // HA and we need to give up master status if we got fenced
          LOG.error("Failed to load/recover state", e);
          throw e;
        }
      } else {
        if (HAUtil.isFederationEnabled(conf)) {
          long epoch = conf.getLong(YarnConfiguration.RM_EPOCH,
              YarnConfiguration.DEFAULT_RM_EPOCH);
          rmContext.setEpoch(epoch);
          LOG.info("Epoch set for Federation: " + epoch);
        }
      }

      super.serviceStart();
    }

    @Override
    protected void serviceStop() throws Exception {

      super.serviceStop();

      DefaultMetricsSystem.shutdown();
      // unregister rmnmInfo bean
      if (rmnmInfo != null) {
        rmnmInfo.unregister();
      }
      if (rmContext != null) {
        RMStateStore store = rmContext.getStateStore();
        try {
          if (null != store) {
            store.close();
          }
        } catch (Exception e) {
          LOG.error("Error closing store.", e);
        }
      }

    }
  }

  @Private
  private class RMFatalEventDispatcher implements EventHandler<RMFatalEvent> {
    @Override
    public void handle(RMFatalEvent event) {
      LOG.error("Received " + event);

      if (HAUtil.isHAEnabled(getConfig())) {
        // If we're in an HA config, the right answer is always to go into
        // standby.
        LOG.warn("Transitioning the resource manager to standby.");
        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 启动 StandByTransitionRunnable 线程
         */
        handleTransitionToStandByInNewThread();
      } else {
        // If we're stand-alone, we probably want to shut down, but the if and
        // how depends on the event.
        switch(event.getType()) {
        case STATE_STORE_FENCED:
          LOG.error(FATAL, "State store fenced even though the resource " +
              "manager is not configured for high availability. Shutting " +
              "down this resource manager to protect the integrity of the " +
              "state store.");
          ExitUtil.terminate(1, event.getExplanation());
          break;
        case STATE_STORE_OP_FAILED:
          if (YarnConfiguration.shouldRMFailFast(getConfig())) {
            LOG.error(FATAL, "Shutting down the resource manager because a " +
                "state store operation failed, and the resource manager is " +
                "configured to fail fast. See the yarn.fail-fast and " +
                "yarn.resourcemanager.fail-fast properties.");
            ExitUtil.terminate(1, event.getExplanation());
          } else {
            LOG.warn("Ignoring state store operation failure because the " +
                "resource manager is not configured to fail fast. See the " +
                "yarn.fail-fast and yarn.resourcemanager.fail-fast " +
                "properties.");
          }
          break;
        default:
          LOG.error(FATAL, "Shutting down the resource manager.");
          ExitUtil.terminate(1, event.getExplanation());
        }
      }
    }
  }

  /**
   * Transition to standby state in a new thread. The transition operation is
   * asynchronous to avoid deadlock caused by cyclic dependency.
   */
  private void handleTransitionToStandByInNewThread() {
    Thread standByTransitionThread =
        new Thread(activeServices.standByTransitionRunnable);
    standByTransitionThread.setName("StandByTransitionThread");
    standByTransitionThread.start();
  }

  /**
   * The class to transition RM to standby state. The same
   * {@link StandByTransitionRunnable} object could be used in multiple threads,
   * but runs only once. That's because RM can go back to active state after
   * transition to standby state, the same runnable in the old context can't
   * transition RM to standby state again. A new runnable is created every time
   * RM transitions to active state.
   */
  private class StandByTransitionRunnable implements Runnable {
    // The atomic variable to make sure multiple threads with the same runnable
    // run only once.
    private final AtomicBoolean hasAlreadyRun = new AtomicBoolean(false);

    @Override
    public void run() {
      // Run this only once, even if multiple threads end up triggering
      // this simultaneously.
      if (hasAlreadyRun.getAndSet(true)) {
        return;
      }

      if (rmContext.isHAEnabled()) {
        try {
          // Transition to standby and reinit active services
          LOG.info("Transitioning RM to Standby mode");
          transitionToStandby(true);
          EmbeddedElector elector = rmContext.getLeaderElectorService();
          if (elector != null) {
            elector.rejoinElection();
          }
        } catch (Exception e) {
          LOG.error(FATAL, "Failed to transition RM to Standby mode.", e);
          ExitUtil.terminate(1, e);
        }
      }
    }
  }

  @Private
  public static final class ApplicationEventDispatcher implements
      EventHandler<RMAppEvent> {

    private final RMContext rmContext;

    public ApplicationEventDispatcher(RMContext rmContext) {
      this.rmContext = rmContext;
    }

    @Override
    public void handle(RMAppEvent event) {
      ApplicationId appID = event.getApplicationId();
      RMApp rmApp = this.rmContext.getRMApps().get(appID);
      if (rmApp != null) {
        try {
          rmApp.handle(event);
        } catch (Throwable t) {
          LOG.error("Error in handling event type " + event.getType()
              + " for application " + appID, t);
        }
      }
    }
  }

  @Private
  public static final class ApplicationAttemptEventDispatcher implements
      EventHandler<RMAppAttemptEvent> {

    private final RMContext rmContext;

    public ApplicationAttemptEventDispatcher(RMContext rmContext) {
      this.rmContext = rmContext;
    }

    @Override
    public void handle(RMAppAttemptEvent event) {
      ApplicationAttemptId appAttemptId = event.getApplicationAttemptId();
      ApplicationId appId = appAttemptId.getApplicationId();
      //todo 每一个appid都有其对应的状态机
      RMApp rmApp = this.rmContext.getRMApps().get(appId);
      if (rmApp != null) {
        RMAppAttempt rmAppAttempt = rmApp.getRMAppAttempt(appAttemptId);
        if (rmAppAttempt != null) {
          try {
            rmAppAttempt.handle(event);
          } catch (Throwable t) {
            LOG.error("Error in handling event type " + event.getType()
                + " for applicationAttempt " + appAttemptId, t);
          }
        } else if (rmApp.getApplicationSubmissionContext() != null
            && rmApp.getApplicationSubmissionContext()
            .getKeepContainersAcrossApplicationAttempts()
            && event.getType() == RMAppAttemptEventType.CONTAINER_FINISHED) {
          // For work-preserving AM restart, failed attempts are still
          // capturing CONTAINER_FINISHED events and record the finished
          // containers which will be used by current attempt.
          // We just keep 'yarn.resourcemanager.am.max-attempts' in
          // RMStateStore. If the finished container's attempt is deleted, we
          // use the first attempt in app.attempts to deal with these events.

          RMAppAttempt previousFailedAttempt =
              rmApp.getAppAttempts().values().iterator().next();
          if (previousFailedAttempt != null) {
            try {
              LOG.debug("Event " + event.getType() + " handled by "
                  + previousFailedAttempt);
              previousFailedAttempt.handle(event);
            } catch (Throwable t) {
              LOG.error("Error in handling event type " + event.getType()
                  + " for applicationAttempt " + appAttemptId
                  + " with " + previousFailedAttempt, t);
            }
          } else {
            LOG.error("Event " + event.getType()
                + " not handled, because previousFailedAttempt is null");
          }
        }
      }
    }
  }

  @Private
  public static final class NodeEventDispatcher implements
      EventHandler<RMNodeEvent> {

    private final RMContext rmContext;

    public NodeEventDispatcher(RMContext rmContext) {
      this.rmContext = rmContext;
    }

    @Override
    public void handle(RMNodeEvent event) {
      NodeId nodeId = event.getNodeId();
      RMNode node = this.rmContext.getRMNodes().get(nodeId);
      if (node != null) {
        try {
          ((EventHandler<RMNodeEvent>) node).handle(event);
        } catch (Throwable t) {
          LOG.error("Error in handling event type " + event.getType()
              + " for node " + nodeId, t);
        }
      }
    }
  }

  /**
   * Return a HttpServer.Builder that the journalnode / namenode / secondary
   * namenode can use to initialize their HTTP / HTTPS server.
   *
   * @param conf configuration object
   * @param httpAddr HTTP address
   * @param httpsAddr HTTPS address
   * @param name  Name of the server
   * @throws IOException from Builder
   * @return builder object
   */
  public static HttpServer2.Builder httpServerTemplateForRM(Configuration conf,
      final InetSocketAddress httpAddr, final InetSocketAddress httpsAddr,
      String name) throws IOException {
    HttpServer2.Builder builder = new HttpServer2.Builder().setName(name)
        .setConf(conf).setSecurityEnabled(false);

    if (httpAddr.getPort() == 0) {
      builder.setFindPort(true);
    }

    URI uri = URI.create("http://" + NetUtils.getHostPortString(httpAddr));
    builder.addEndpoint(uri);
    LOG.info("Starting Web-server for " + name + " at: " + uri);

    return builder;
  }

  protected void startWepApp() {
    Map<String, String> serviceConfig = null;
    Configuration conf = getConfig();

    RMWebAppUtil.setupSecurityAndFilters(conf,
        getClientRMService().rmDTSecretManager);

    Map<String, String> params = new HashMap<String, String>();
    if (getConfig().getBoolean(YarnConfiguration.YARN_API_SERVICES_ENABLE,
        false)) {
      String apiPackages = "org.apache.hadoop.yarn.service.webapp;" +
          "org.apache.hadoop.yarn.webapp";
      params.put("com.sun.jersey.config.property.resourceConfigClass",
          "com.sun.jersey.api.core.PackagesResourceConfig");
      params.put("com.sun.jersey.config.property.packages", apiPackages);
    }

    Builder<ResourceManager> builder =
        WebApps
            .$for("cluster", ResourceManager.class, this,
                "ws")
            .with(conf)
            .withServlet("API-Service", "/app/*",
                ServletContainer.class, params, false)
            .withHttpSpnegoPrincipalKey(
                YarnConfiguration.RM_WEBAPP_SPNEGO_USER_NAME_KEY)
            .withHttpSpnegoKeytabKey(
                YarnConfiguration.RM_WEBAPP_SPNEGO_KEYTAB_FILE_KEY)
            .withCSRFProtection(YarnConfiguration.RM_CSRF_PREFIX)
            .withXFSProtection(YarnConfiguration.RM_XFS_PREFIX)
            .at(webAppAddress);
    String proxyHostAndPort = rmContext.getProxyHostAndPort(conf);
    if(WebAppUtils.getResolvedRMWebAppURLWithoutScheme(conf).
        equals(proxyHostAndPort)) {
      if (HAUtil.isHAEnabled(conf)) {
        fetcher = new AppReportFetcher(conf);
      } else {
        fetcher = new AppReportFetcher(conf, getClientRMService());
      }
      builder.withServlet(ProxyUriUtils.PROXY_SERVLET_NAME,
          ProxyUriUtils.PROXY_PATH_SPEC, WebAppProxyServlet.class);
      builder.withAttribute(WebAppProxy.FETCHER_ATTRIBUTE, fetcher);
      String[] proxyParts = proxyHostAndPort.split(":");
      builder.withAttribute(WebAppProxy.PROXY_HOST_ATTRIBUTE, proxyParts[0]);
    }

    WebAppContext uiWebAppContext = null;
    if (getConfig().getBoolean(YarnConfiguration.YARN_WEBAPP_UI2_ENABLE,
        YarnConfiguration.DEFAULT_YARN_WEBAPP_UI2_ENABLE)) {
      String onDiskPath = getConfig()
          .get(YarnConfiguration.YARN_WEBAPP_UI2_WARFILE_PATH);

      uiWebAppContext = new WebAppContext();
      uiWebAppContext.setContextPath(UI2_WEBAPP_NAME);

      if (null == onDiskPath) {
        String war = "hadoop-yarn-ui-" + VersionInfo.getVersion() + ".war";
        URL url = getClass().getClassLoader().getResource(war);

        if (null == url) {
          onDiskPath = getWebAppsPath("ui2");
        } else {
          onDiskPath = url.getFile();
        }
      }
      if (onDiskPath == null || onDiskPath.isEmpty()) {
          LOG.error("No war file or webapps found for ui2 !");
      } else {
        if (onDiskPath.endsWith(".war")) {
          uiWebAppContext.setWar(onDiskPath);
          LOG.info("Using war file at: " + onDiskPath);
        } else {
          uiWebAppContext.setResourceBase(onDiskPath);
          LOG.info("Using webapps at: " + onDiskPath);
        }
      }
    }

    builder.withAttribute(IsResourceManagerActiveServlet.RM_ATTRIBUTE, this);
    builder.withServlet(IsResourceManagerActiveServlet.SERVLET_NAME,
        IsResourceManagerActiveServlet.PATH_SPEC,
        IsResourceManagerActiveServlet.class);

    webApp = builder.start(new RMWebApp(this), uiWebAppContext);
  }

  private String getWebAppsPath(String appName) {
    URL url = getClass().getClassLoader().getResource("webapps/" + appName);
    if (url == null) {
      return "";
    }
    return url.toString();
  }

  /**
   * Helper method to create and init {@link #activeServices}. This creates an
   * instance of {@link RMActiveServices} and initializes it.
   *
   * @param fromActive Indicates if the call is from the active state transition
   *                   or the RM initialization.
   */
  protected void createAndInitActiveServices(boolean fromActive) {
    //todo 注释： RMActiveServices 内部包装了 Active RM 应该启动的服务
    activeServices = new RMActiveServices(this);
    activeServices.fromActive = fromActive;
    activeServices.init(conf);
  }

  /**
   * Helper method to start {@link #activeServices}.
   * @throws Exception
   */
  void startActiveServices() throws Exception {
    if (activeServices != null) {
      clusterTimeStamp = System.currentTimeMillis();
      activeServices.start();
    }
  }

  /**
   * Helper method to stop {@link #activeServices}.
   * @throws Exception
   */
  void stopActiveServices() {
    if (activeServices != null) {
      activeServices.stop();
      activeServices = null;
    }
  }

  void reinitialize(boolean initialize) {
    ClusterMetrics.destroy();
    QueueMetrics.clearQueueMetrics();
    getResourceScheduler().resetSchedulerMetrics();
    if (initialize) {
      resetRMContext();
      createAndInitActiveServices(true);
    }
  }

  @VisibleForTesting
  protected boolean areActiveServicesRunning() {
    return activeServices != null && activeServices.isInState(STATE.STARTED);
  }

  synchronized void transitionToActive() throws Exception {
    if (rmContext.getHAServiceState() == HAServiceProtocol.HAServiceState.ACTIVE) {
      LOG.info("Already in active state");
      return;
    }
    LOG.info("Transitioning to active state");

    this.rmLoginUGI.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try {
          startActiveServices();
          return null;
        } catch (Exception e) {
          reinitialize(true);
          throw e;
        }
      }
    });

    rmContext.setHAServiceState(HAServiceProtocol.HAServiceState.ACTIVE);
    LOG.info("Transitioned to active state");
  }

  synchronized void transitionToStandby(boolean initialize)
      throws Exception {
    if (rmContext.getHAServiceState() ==
        HAServiceProtocol.HAServiceState.STANDBY) {
      LOG.info("Already in standby state");
      return;
    }

    LOG.info("Transitioning to standby state");
    HAServiceState state = rmContext.getHAServiceState();
    rmContext.setHAServiceState(HAServiceProtocol.HAServiceState.STANDBY);
    if (state == HAServiceProtocol.HAServiceState.ACTIVE) {
      stopActiveServices();
      reinitialize(initialize);
    }
    LOG.info("Transitioned to standby state");
  }

  @Override
  protected void serviceStart() throws Exception {
    //todo *  注释： 刚启动，自动成为 Standby
    if (this.rmContext.isHAEnabled()) {
      transitionToStandby(false);
    }
    //todo 注释： 开启webapp服务，主要是用户认证服务
    startWepApp();
    if (getConfig().getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER,
        false)) {
      int port = webApp.port();
      WebAppUtils.setRMWebAppPort(conf, port);
    }
    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 启动一系列各种服务
     *  最后把 CompositeService 里面的所有服务全部调用 start 方法
     *  其中的一个子 service =  EmbeddedElector 选举！
     */
    super.serviceStart();
/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 切换成 Active，争抢实现！
 *  如果不是 HA 模式，则自己直接成为 active，不用使用 StandByTransitionRunnable 来进行了
 */
    // Non HA case, start after RM services are started.
    if (!this.rmContext.isHAEnabled()) {
      transitionToActive();
    }
  }
  
  protected void doSecureLogin() throws IOException {
	InetSocketAddress socAddr = getBindAddress(conf);
    SecurityUtil.login(this.conf, YarnConfiguration.RM_KEYTAB,
        YarnConfiguration.RM_PRINCIPAL, socAddr.getHostName());

    // if security is enable, set rmLoginUGI as UGI of loginUser
    if (UserGroupInformation.isSecurityEnabled()) {
      this.rmLoginUGI = UserGroupInformation.getLoginUser();
    }
  }

  @Override
  protected void serviceStop() throws Exception {
    if (webApp != null) {
      webApp.stop();
    }
    if (fetcher != null) {
      fetcher.stop();
    }
    if (configurationProvider != null) {
      configurationProvider.close();
    }
    super.serviceStop();
    if (zkManager != null) {
      zkManager.close();
    }
    transitionToStandby(false);
    rmContext.setHAServiceState(HAServiceState.STOPPING);
  }
  
  protected ResourceTrackerService createResourceTrackerService() {
    return new ResourceTrackerService(this.rmContext, this.nodesListManager,
        this.nmLivelinessMonitor,
        this.rmContext.getContainerTokenSecretManager(),
        this.rmContext.getNMTokenSecretManager());
  }

  protected ClientRMService createClientRMService() {
    return new ClientRMService(this.rmContext, scheduler, this.rmAppManager,
        this.applicationACLsManager, this.queueACLsManager,
        this.rmContext.getRMDelegationTokenSecretManager());
  }

  protected ApplicationMasterService createApplicationMasterService() {
    Configuration config = this.rmContext.getYarnConfiguration();
    if (isOpportunisticSchedulingEnabled(conf)) {
      if (YarnConfiguration.isDistSchedulingEnabled(config) &&
          !YarnConfiguration
              .isOpportunisticContainerAllocationEnabled(config)) {
        throw new YarnRuntimeException(
            "Invalid parameters: opportunistic container allocation has to " +
                "be enabled when distributed scheduling is enabled.");
      }
      OpportunisticContainerAllocatorAMService
          oppContainerAllocatingAMService =
          new OpportunisticContainerAllocatorAMService(this.rmContext,
              scheduler);
      this.rmContext.setContainerQueueLimitCalculator(
          oppContainerAllocatingAMService.getNodeManagerQueueLimitCalculator());
      return oppContainerAllocatingAMService;
    }
    return new ApplicationMasterService(this.rmContext, scheduler);
  }

  protected AdminService createAdminService() {
    return new AdminService(this);
  }

  protected RMSecretManagerService createRMSecretManagerService() {
    return new RMSecretManagerService(conf, rmContext);
  }

  private boolean isOpportunisticSchedulingEnabled(Configuration conf) {
    return YarnConfiguration.isOpportunisticContainerAllocationEnabled(conf)
        || YarnConfiguration.isDistSchedulingEnabled(conf);
  }

  /**
   * Create RMDelegatedNodeLabelsUpdater based on configuration.
   */
  protected RMDelegatedNodeLabelsUpdater createRMDelegatedNodeLabelsUpdater() {
    if (conf.getBoolean(YarnConfiguration.NODE_LABELS_ENABLED,
            YarnConfiguration.DEFAULT_NODE_LABELS_ENABLED)
        && YarnConfiguration.isDelegatedCentralizedNodeLabelConfiguration(
            conf)) {
      return new RMDelegatedNodeLabelsUpdater(rmContext);
    } else {
      return null;
    }
  }

  @Private
  public ClientRMService getClientRMService() {
    return this.clientRM;
  }

  /**
   * return the scheduler.
   * @return the scheduler for the Resource Manager.
   */
  @Private
  public ResourceScheduler getResourceScheduler() {
    return this.scheduler;
  }

  /**
   * return the resource tracking component.
   * @return the resource tracking component.
   */
  @Private
  public ResourceTrackerService getResourceTrackerService() {
    return this.resourceTracker;
  }

  @Private
  public ApplicationMasterService getApplicationMasterService() {
    return this.masterService;
  }

  @Private
  public ApplicationACLsManager getApplicationACLsManager() {
    return this.applicationACLsManager;
  }

  @Private
  public QueueACLsManager getQueueACLsManager() {
    return this.queueACLsManager;
  }

  @Private
  @VisibleForTesting
  public FederationStateStoreService getFederationStateStoreService() {
    return this.federationStateStoreService;
  }

  @Private
  WebApp getWebapp() {
    return this.webApp;
  }

  @Override
  public void recover(RMState state) throws Exception {
    // recover RMdelegationTokenSecretManager
    rmContext.getRMDelegationTokenSecretManager().recover(state);

    // recover AMRMTokenSecretManager
    rmContext.getAMRMTokenSecretManager().recover(state);

    // recover reservations
    if (reservationSystem != null) {
      reservationSystem.recover(state);
    }
    // recover applications
    rmAppManager.recover(state);

    setSchedulerRecoveryStartAndWaitTime(state, conf);
  }

  public static void main(String argv[]) {
    //todo // TODO_MA 注释： 为主线程注册一个 UncaughtException 处理器，设置在线程因未捕获异常而突然终止时调用的默认处理程序
    Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
    StringUtils.startupShutdownMessage(ResourceManager.class, argv, LOG);
    try {
      //todo // TODO_MA 注释： xml properties
      Configuration conf = new YarnConfiguration();
      GenericOptionsParser hParser = new GenericOptionsParser(conf, argv);
      argv = hParser.getRemainingArgs();
      // If -format-state-store, then delete RMStateStore; else startup normally
      if (argv.length >= 1) {
        if (argv[0].equals("-format-state-store")) {
          deleteRMStateStore(conf);
        } else if (argv[0].equals("-format-conf-store")) {
          deleteRMConfStore(conf);
        } else if (argv[0].equals("-remove-application-from-state-store")
            && argv.length == 2) {
          removeApplication(conf, argv[1]);
        } else {
          printUsage(System.err);
        }
      } else {
        ResourceManager resourceManager = new ResourceManager();
        //todo
        // TODO_MA 注释： 把 ResourceManager 的 CompositeService 的 shutDownHook
        // TODO_MA 注释： 添加到一个统一的 ShutdownHookManager
        ShutdownHookManager.get().addShutdownHook(
          new CompositeServiceShutdownHook(resourceManager),
          SHUTDOWN_HOOK_PRIORITY);
        //todo 注释： 第一件重要的事情： 初始化各种服务
        resourceManager.init(conf);
        //todo 注释： 第二件重要的事情： 启动各种服务
        resourceManager.start();
      }
    } catch (Throwable t) {
      LOG.error(FATAL, "Error starting ResourceManager", t);
      System.exit(-1);
    }
  }

  /**
   * Register the handlers for alwaysOn services
   */
  private Dispatcher setupDispatcher() {
    Dispatcher dispatcher = createDispatcher();
    //todo 注册RMFatalEventType对应的handler
    dispatcher.register(RMFatalEventType.class,
        new ResourceManager.RMFatalEventDispatcher());
    return dispatcher;
  }

  private void resetRMContext() {
    RMContextImpl rmContextImpl = new RMContextImpl();
    // transfer service context to new RM service Context
    rmContextImpl.setServiceContext(rmContext.getServiceContext());

    // reset dispatcher
    Dispatcher dispatcher = setupDispatcher();
    ((Service) dispatcher).init(this.conf);
    ((Service) dispatcher).start();
    removeService((Service) rmDispatcher);
    // Need to stop previous rmDispatcher before assigning new dispatcher
    // otherwise causes "AsyncDispatcher event handler" thread leak
    ((Service) rmDispatcher).stop();
    rmDispatcher = dispatcher;
    addIfService(rmDispatcher);
    rmContextImpl.setDispatcher(dispatcher);

    rmContext = rmContextImpl;
  }

  private void setSchedulerRecoveryStartAndWaitTime(RMState state,
      Configuration conf) {
    if (!state.getApplicationState().isEmpty()) {
      long waitTime =
          conf.getLong(YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_SCHEDULING_WAIT_MS,
            YarnConfiguration.DEFAULT_RM_WORK_PRESERVING_RECOVERY_SCHEDULING_WAIT_MS);
      rmContext.setSchedulerRecoveryStartAndWaitTime(waitTime);
    }
  }

  /**
   * Retrieve RM bind address from configuration
   * 
   * @param conf
   * @return InetSocketAddress
   */
  public static InetSocketAddress getBindAddress(Configuration conf) {
    return conf.getSocketAddr(YarnConfiguration.RM_ADDRESS,
      YarnConfiguration.DEFAULT_RM_ADDRESS, YarnConfiguration.DEFAULT_RM_PORT);
  }

  /**
   * Deletes the RMStateStore
   *
   * @param conf
   * @throws Exception
   */
  @VisibleForTesting
  static void deleteRMStateStore(Configuration conf) throws Exception {
    RMStateStore rmStore = RMStateStoreFactory.getStore(conf);
    rmStore.setResourceManager(new ResourceManager());
    rmStore.init(conf);
    rmStore.start();
    try {
      LOG.info("Deleting ResourceManager state store...");
      rmStore.deleteStore();
      LOG.info("State store deleted");
    } finally {
      rmStore.stop();
    }
  }

  /**
   * Deletes the YarnConfigurationStore.
   *
   * @param conf
   * @throws Exception
   */
  @VisibleForTesting
  static void deleteRMConfStore(Configuration conf) throws Exception {
    ResourceManager rm = new ResourceManager();
    rm.conf = conf;
    ResourceScheduler scheduler = rm.createScheduler();
    RMContextImpl rmContext = new RMContextImpl();
    rmContext.setResourceManager(rm);

    boolean isConfigurationMutable = false;
    String confProviderStr = conf.get(
        YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
        YarnConfiguration.DEFAULT_CONFIGURATION_STORE);
    switch (confProviderStr) {
    case YarnConfiguration.MEMORY_CONFIGURATION_STORE:
    case YarnConfiguration.LEVELDB_CONFIGURATION_STORE:
    case YarnConfiguration.ZK_CONFIGURATION_STORE:
    case YarnConfiguration.FS_CONFIGURATION_STORE:
      isConfigurationMutable = true;
      break;
    default:
    }

    if (scheduler instanceof MutableConfScheduler && isConfigurationMutable) {
      YarnConfigurationStore confStore = YarnConfigurationStoreFactory
          .getStore(conf);
      confStore.initialize(conf, conf, rmContext);
      confStore.format();
    } else {
      System.out.println("Scheduler Configuration format only " +
          "supported by MutableConfScheduler.");
    }
  }

  @VisibleForTesting
  static void removeApplication(Configuration conf, String applicationId)
      throws Exception {
    RMStateStore rmStore = RMStateStoreFactory.getStore(conf);
    rmStore.setResourceManager(new ResourceManager());
    rmStore.init(conf);
    rmStore.start();
    try {
      ApplicationId removeAppId = ApplicationId.fromString(applicationId);
      LOG.info("Deleting application " + removeAppId + " from state store");
      rmStore.removeApplication(removeAppId);
      LOG.info("Application is deleted from state store");
    } finally {
      rmStore.stop();
    }
  }

  private static void printUsage(PrintStream out) {
    out.println("Usage: yarn resourcemanager [-format-state-store]");
    out.println("                            "
        + "[-remove-application-from-state-store <appId>]");
    out.println("                            "
        + "[-format-conf-store]" + "\n");
  }

  protected RMAppLifetimeMonitor createRMAppLifetimeMonitor() {
    return new RMAppLifetimeMonitor(this.rmContext);
  }

  /**
   * Register ResourceManagerMXBean.
   */
  private void registerMXBean() {
    MBeans.register("ResourceManager", "ResourceManager", this);
  }

  @Override
  public boolean isSecurityEnabled() {
    return UserGroupInformation.isSecurityEnabled();
  }
}
