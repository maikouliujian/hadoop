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

package org.apache.hadoop.mapreduce.v2.app.launcher;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.ShuffleHandler;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerLaunchedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.concurrent.HadoopThreadPoolExecutor;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersResponse;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.SignalContainerCommand;
import org.apache.hadoop.yarn.client.api.impl.ContainerManagementProtocolProxy;
import org.apache.hadoop.yarn.client.api.impl.ContainerManagementProtocolProxy.ContainerManagementProtocolProxyData;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for launching of containers.
 */
public class ContainerLauncherImpl extends AbstractService implements
    ContainerLauncher {

  static final Logger LOG =
      LoggerFactory.getLogger(ContainerLauncherImpl.class);

  private ConcurrentHashMap<ContainerId, Container> containers = 
    new ConcurrentHashMap<ContainerId, Container>(); 
  private final AppContext context;
  protected ThreadPoolExecutor launcherPool;
  protected int initialPoolSize;
  private int limitOnPoolSize;
  private Thread eventHandlingThread;
  protected BlockingQueue<ContainerLauncherEvent> eventQueue =
      new LinkedBlockingQueue<ContainerLauncherEvent>();
  private final AtomicBoolean stopped;
  private ContainerManagementProtocolProxy cmProxy;

  private Container getContainer(ContainerLauncherEvent event) {
    ContainerId id = event.getContainerID();
    Container c = containers.get(id);
    if(c == null) {
      c = new Container(event.getTaskAttemptID(), event.getContainerID(),
          event.getContainerMgrAddress());
      Container old = containers.putIfAbsent(id, c);
      if(old != null) {
        c = old;
      }
    }
    return c;
  }
  
  private void removeContainerIfDone(ContainerId id) {
    Container c = containers.get(id);
    if(c != null && c.isCompletelyDone()) {
      containers.remove(id);
    }
  }
  
  private enum ContainerState {
    PREP, FAILED, RUNNING, DONE, KILLED_BEFORE_LAUNCH
  }

  private class Container {
    private ContainerState state;
    // store enough information to be able to cleanup the container
    private TaskAttemptId taskAttemptID;
    private ContainerId containerID;
    final private String containerMgrAddress;
    
    public Container(TaskAttemptId taId, ContainerId containerID,
        String containerMgrAddress) {
      this.state = ContainerState.PREP;
      this.taskAttemptID = taId;
      this.containerMgrAddress = containerMgrAddress;
      this.containerID = containerID;
    }
    
    public synchronized boolean isCompletelyDone() {
      return state == ContainerState.DONE || state == ContainerState.FAILED;
    }

    public synchronized void done() {
      state = ContainerState.DONE;
    }

    @SuppressWarnings("unchecked")
    public synchronized void launch(ContainerRemoteLaunchEvent event) {
      LOG.info("Launching " + taskAttemptID);
      if(this.state == ContainerState.KILLED_BEFORE_LAUNCH) {
        state = ContainerState.DONE;
        sendContainerLaunchFailedMsg(taskAttemptID, 
            "Container was killed before it was launched");
        return;
      }
      
      ContainerManagementProtocolProxyData proxy = null;
      try {
        /*************************************************
         * TODO 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 获取与NM通信代理对象
         */
        proxy = getCMProxy(containerMgrAddress, containerID);
        // TODO 注释： 上下文对象
        // TODO 注释： 最重要的信息： 启动 JVM 的 shell 命令：  java YarnChild
        // Construct the actual Container
        ContainerLaunchContext containerLaunchContext =
          event.getContainerLaunchContext();
        // TODO 注释： 构建启动 Container 的 Request 请求对象
        // Now launch the actual container
        StartContainerRequest startRequest =
            StartContainerRequest.newInstance(containerLaunchContext,
              event.getContainerToken());
        List<StartContainerRequest> list = new ArrayList<StartContainerRequest>();
        list.add(startRequest);
        StartContainersRequest requestList = StartContainersRequest.newInstance(list);
        //todo 启动container
        /*************************************************
         * TODO 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 启动 MapTask 或者 ReduceTask 的 container
         *  requestList 对象中，是包含了 ContainerLaunchContext 的。
         */
        StartContainersResponse response =
            proxy.getContainerManagementProtocol().startContainers(requestList);
        if (response.getFailedRequests() != null
            && response.getFailedRequests().containsKey(containerID)) {
          throw response.getFailedRequests().get(containerID).deSerialize();
        }
        ByteBuffer portInfo =
            response.getAllServicesMetaData().get(
                ShuffleHandler.MAPREDUCE_SHUFFLE_SERVICEID);
        int port = -1;
        if(portInfo != null) {
          port = ShuffleHandler.deserializeMetaData(portInfo);
        }
        LOG.info("Shuffle port returned by ContainerManager for "
            + taskAttemptID + " : " + port);

        if(port < 0) {
          this.state = ContainerState.FAILED;
          throw new IllegalStateException("Invalid shuffle port number "
              + port + " returned for " + taskAttemptID);
        }

        /*************************************************
         * TODO 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： TaskAttemptEventType.TA_CONTAINER_LAUNCHED
         */
        // after launching, send launched event to task attempt to move
        // it from ASSIGNED to RUNNING state
        context.getEventHandler().handle(
            new TaskAttemptContainerLaunchedEvent(taskAttemptID, port));
        this.state = ContainerState.RUNNING;
      } catch (Throwable t) {
        String message = "Container launch failed for " + containerID + " : "
            + StringUtils.stringifyException(t);
        this.state = ContainerState.FAILED;
        sendContainerLaunchFailedMsg(taskAttemptID, message);
      } finally {
        if (proxy != null) {
          cmProxy.mayBeCloseProxy(proxy);
        }
      }
    }

    public void kill() {
      kill(false);
    }

    @SuppressWarnings("unchecked")
    public synchronized void kill(boolean dumpThreads) {

      if(this.state == ContainerState.PREP) {
        this.state = ContainerState.KILLED_BEFORE_LAUNCH;
      } else if (!isCompletelyDone()) {
        LOG.info("KILLING " + taskAttemptID);

        ContainerManagementProtocolProxyData proxy = null;
        try {
          proxy = getCMProxy(this.containerMgrAddress, this.containerID);

          if (dumpThreads) {
            final SignalContainerRequest request = SignalContainerRequest
                .newInstance(containerID,
                    SignalContainerCommand.OUTPUT_THREAD_DUMP);
            proxy.getContainerManagementProtocol().signalToContainer(request);
          }

          // kill the remote container if already launched
          List<ContainerId> ids = new ArrayList<ContainerId>();
          ids.add(this.containerID);
          StopContainersRequest request = StopContainersRequest.newInstance(ids);
          StopContainersResponse response =
              proxy.getContainerManagementProtocol().stopContainers(request);
          if (response.getFailedRequests() != null
              && response.getFailedRequests().containsKey(this.containerID)) {
            throw response.getFailedRequests().get(this.containerID)
              .deSerialize();
          }
        } catch (Throwable t) {
          // ignore the cleanup failure
          String message = "cleanup failed for container "
              + this.containerID + " : "
              + StringUtils.stringifyException(t);
          context.getEventHandler()
              .handle(
                  new TaskAttemptDiagnosticsUpdateEvent(this.taskAttemptID,
                      message));
          LOG.warn(message);
        } finally {
          if (proxy != null) {
            cmProxy.mayBeCloseProxy(proxy);
          }
        }
        this.state = ContainerState.DONE;
      }
      // after killing, send killed event to task attempt
      context.getEventHandler().handle(
          new TaskAttemptEvent(this.taskAttemptID,
              TaskAttemptEventType.TA_CONTAINER_CLEANED));
    }
  }

  public ContainerLauncherImpl(AppContext context) {
    super(ContainerLauncherImpl.class.getName());
    this.context = context;
    this.stopped = new AtomicBoolean(false);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // TODO 注释： 线程池上限线程数 = 500
    this.limitOnPoolSize = conf.getInt(
        MRJobConfig.MR_AM_CONTAINERLAUNCHER_THREAD_COUNT_LIMIT,
        MRJobConfig.DEFAULT_MR_AM_CONTAINERLAUNCHER_THREAD_COUNT_LIMIT);
    LOG.info("Upper limit on the thread pool size is " + this.limitOnPoolSize);
    // TODO 注释： 初始线程数 = 10
    this.initialPoolSize = conf.getInt(
        MRJobConfig.MR_AM_CONTAINERLAUNCHER_THREADPOOL_INITIAL_SIZE,
        MRJobConfig.DEFAULT_MR_AM_CONTAINERLAUNCHER_THREADPOOL_INITIAL_SIZE);
    LOG.info("The thread pool initial size is " + this.initialPoolSize);

    super.serviceInit(conf);
    // TODO 注释： 最重要的代码，就是这句初始化获取通信协议代理对象
    //todo ContainerManagementProtocol 是mrappmaster和nodemanager之间的通信协议
    cmProxy = new ContainerManagementProtocolProxy(conf);
  }

  protected void serviceStart() throws Exception {
    // TODO 注释： 线程工厂
    ThreadFactory tf = new ThreadFactoryBuilder().setNameFormat(
        "ContainerLauncher #%d").setDaemon(true).build();

    // Start with a default core-pool size of 10 and change it dynamically.
    launcherPool = new HadoopThreadPoolExecutor(initialPoolSize,
        Integer.MAX_VALUE, 1, TimeUnit.HOURS,
        new LinkedBlockingQueue<Runnable>(),
        tf);
    eventHandlingThread = new Thread() {
      @Override
      public void run() {
        ContainerLauncherEvent event = null;
        Set<String> allNodes = new HashSet<String>();

        while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
          try {
            event = eventQueue.take();
          } catch (InterruptedException e) {
            if (!stopped.get()) {
              LOG.error("Returning, interrupted : " + e);
            }
            return;
          }
          allNodes.add(event.getContainerMgrAddress());

          int poolSize = launcherPool.getCorePoolSize();

          // See if we need up the pool size only if haven't reached the
          // maximum limit yet.
          if (poolSize != limitOnPoolSize) {

            // nodes where containers will run at *this* point of time. This is
            // *not* the cluster size and doesn't need to be.
            int numNodes = allNodes.size();
            int idealPoolSize = Math.min(limitOnPoolSize, numNodes);

            if (poolSize < idealPoolSize) {
              // Bump up the pool size to idealPoolSize+initialPoolSize, the
              // later is just a buffer so we are not always increasing the
              // pool-size
              int newPoolSize = Math.min(limitOnPoolSize, idealPoolSize
                  + initialPoolSize);
              LOG.info("Setting ContainerLauncher pool size to " + newPoolSize
                  + " as number-of-nodes to talk to is " + numNodes);
              launcherPool.setCorePoolSize(newPoolSize);
            }
          }

          // the events from the queue are handled in parallel
          // using a thread pool
          launcherPool.execute(createEventProcessor(event));

          // TODO: Group launching of multiple containers to a single
          // NodeManager into a single connection
        }
      }
    };
    eventHandlingThread.setName("ContainerLauncher Event Handler");
    eventHandlingThread.start();
    super.serviceStart();
  }

  private void shutdownAllContainers() {
    for (Container ct : this.containers.values()) {
      if (ct != null) {
        ct.kill();
      }
    }
  }

  protected void serviceStop() throws Exception {
    if (stopped.getAndSet(true)) {
      // return if already stopped
      return;
    }
    // shutdown any containers that might be left running
    shutdownAllContainers();
    if (eventHandlingThread != null) {
      eventHandlingThread.interrupt();
    }
    if (launcherPool != null) {
      launcherPool.shutdownNow();
    }
    super.serviceStop();
  }

  protected EventProcessor createEventProcessor(ContainerLauncherEvent event) {
    return new EventProcessor(event);
  }

  /**
   * Setup and start the container on remote nodemanager.
   */
  class EventProcessor implements Runnable {
    private ContainerLauncherEvent event;

    EventProcessor(ContainerLauncherEvent event) {
      this.event = event;
    }

    @Override
    public void run() {
      LOG.info("Processing the event " + event.toString());

      // Load ContainerManager tokens before creating a connection.
      // TODO: Do it only once per NodeManager.
      ContainerId containerID = event.getContainerID();

      Container c = getContainer(event);
      switch(event.getType()) {
        /*************************************************
         * TODO 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 启动 Container
         */
      case CONTAINER_REMOTE_LAUNCH:
        ContainerRemoteLaunchEvent launchEvent
            = (ContainerRemoteLaunchEvent) event;
        c.launch(launchEvent);
        break;
        /*************************************************
         * TODO 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： kill Contianer
         */
      case CONTAINER_REMOTE_CLEANUP:
        c.kill(event.getDumpContainerThreads());
        break;

      case CONTAINER_COMPLETED:
        c.done();
        break;

      }
      removeContainerIfDone(containerID);
    }
  }
  
  @SuppressWarnings("unchecked")
  void sendContainerLaunchFailedMsg(TaskAttemptId taskAttemptID,
      String message) {
    LOG.error(message);
    context.getEventHandler().handle(
        new TaskAttemptDiagnosticsUpdateEvent(taskAttemptID, message));
    context.getEventHandler().handle(
        new TaskAttemptEvent(taskAttemptID,
            TaskAttemptEventType.TA_CONTAINER_LAUNCH_FAILED));
  }

  @Override
  public void handle(ContainerLauncherEvent event) {
    try {
      eventQueue.put(event);
    } catch (InterruptedException e) {
      throw new YarnRuntimeException(e);
    }
  }
  
  public ContainerManagementProtocolProxy.ContainerManagementProtocolProxyData
      getCMProxy(String containerMgrBindAddr, ContainerId containerId)
          throws IOException {
    return cmProxy.getProxy(containerMgrBindAddr, containerId);
  }
}
