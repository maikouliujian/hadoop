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

package org.apache.hadoop.yarn.server.resourcemanager.amlauncher;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;

/*************************************************
 * TODO 马中华 https://blog.csdn.net/zhongqi2513
 *  注释：
 *  1、一个是队列 masterEvents（）
 *  2、一个是线程 launcherHandlingThread = LauncherThread 消费队列，执行任务，启动 ApplicatoinMaster
 *  任务就是： AMLauncher ， 用来启动 ApplicationMaster
 */
public class ApplicationMasterLauncher extends AbstractService implements
    EventHandler<AMLauncherEvent> {
  private static final Logger LOG = LoggerFactory.getLogger(
      ApplicationMasterLauncher.class);
  private ThreadPoolExecutor launcherPool;
  private LauncherThread launcherHandlingThread;
  
  private final BlockingQueue<Runnable> masterEvents
    = new LinkedBlockingQueue<Runnable>();
  
  protected final RMContext context;
  
  public ApplicationMasterLauncher(RMContext context) {
    super(ApplicationMasterLauncher.class.getName());
    this.context = context;
    /*************************************************
     * TODO 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 初始化一个 LauncherThread
     */
    this.launcherHandlingThread = new LauncherThread();
  }
  /*************************************************
   * TODO 马中华 https://blog.csdn.net/zhongqi2513
   *  注释： AMLauncher 内部有一个线程池，这个线程池的目的，就是为每一个 AM 的启动，启动一个专门的线程来负责启动
   */
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    int threadCount = conf.getInt(
        YarnConfiguration.RM_AMLAUNCHER_THREAD_COUNT,
        YarnConfiguration.DEFAULT_RM_AMLAUNCHER_THREAD_COUNT);
    ThreadFactory tf = new ThreadFactoryBuilder()
        .setNameFormat("ApplicationMasterLauncher #%d")
        .build();
    /*************************************************
     * TODO 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 用来启动 ApplicationMaster 的线程池
     */
    launcherPool = new ThreadPoolExecutor(threadCount, threadCount, 1,
        TimeUnit.HOURS, new LinkedBlockingQueue<Runnable>());
    launcherPool.setThreadFactory(tf);

    Configuration newConf = new YarnConfiguration(conf);
    // TODO 注释： 重试次数：默认 10
    newConf.setInt(CommonConfigurationKeysPublic.
            IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
        conf.getInt(YarnConfiguration.RM_NODEMANAGER_CONNECT_RETRIES,
            YarnConfiguration.DEFAULT_RM_NODEMANAGER_CONNECT_RETRIES));
    // TODO 注释： 设置参数
    setConfig(newConf);
    super.serviceInit(newConf);
  }

  @Override
  protected void serviceStart() throws Exception {

    /*************************************************
     * TODO 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 启动 LauncherThread
     */
    launcherHandlingThread.start();
    super.serviceStart();
  }
  
  protected Runnable createRunnableLauncher(RMAppAttempt application, 
      AMLauncherEventType event) {
    /*************************************************
     * TODO 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 创建 AMLauncher
     */
    Runnable launcher =
        new AMLauncher(context, application, event, getConfig());
    return launcher;
  }
  
  private void launch(RMAppAttempt application) {
    /*************************************************
     * TODO 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 创建一个 ApplicationMaster Launcher
     *  将来会通过一个 EventHandler 来执行这个事件：AMLauncherEventType.LAUNCH 的处理
     */
    Runnable launcher = createRunnableLauncher(application, 
        AMLauncherEventType.LAUNCH);
    /*************************************************
     * TODO 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 加入队列
     *  将 Runnable launcher 加入队列，则等待，处理线程 LauncherThread 来处理
     */
    masterEvents.add(launcher);
  }
  

  @Override
  protected void serviceStop() throws Exception {
    launcherHandlingThread.interrupt();
    try {
      launcherHandlingThread.join();
    } catch (InterruptedException ie) {
      LOG.info(launcherHandlingThread.getName() + " interrupted during join ", 
          ie);    }
    launcherPool.shutdown();
  }

  private class LauncherThread extends Thread {
    
    public LauncherThread() {
      super("ApplicationMaster Launcher");
    }

    @Override
    public void run() {
      while (!this.isInterrupted()) {
        Runnable toLaunch;
        try {
          toLaunch = masterEvents.take();
          launcherPool.execute(toLaunch);
        } catch (InterruptedException e) {
          LOG.warn(this.getClass().getName() + " interrupted. Returning.");
          return;
        }
      }
    }
  }    

  private void cleanup(RMAppAttempt application) {
    Runnable launcher = createRunnableLauncher(application, AMLauncherEventType.CLEANUP);
    masterEvents.add(launcher);
  } 

  //todo 如果rm收到一个application：
  //todo 1、构建一个RMAppImpl状态机来维护此application的状态；
  //todo 2、提交一个START事件给asyncdispatcher，最终由ApplicationMasterLauncher来执行处理
  //todo RMAppAttemptImpl ===> eventHandler.handle(new AMLauncherEvent(AMLauncherEventType.LAUNCH, this));
  @Override
  public synchronized void handle(AMLauncherEvent appEvent) {
    AMLauncherEventType event = appEvent.getType();
    RMAppAttempt application = appEvent.getAppAttempt();
    switch (event) {
    case LAUNCH:
      /*************************************************
       * TODO 马中华 https://blog.csdn.net/zhongqi2513
       *  注释： 要通过 ApplicatoinMasterLauncher 来启动一个 ApplicationMaster
       */
      launch(application);
      break;
    case CLEANUP:
      cleanup(application);
      break;
    default:
      break;
    }
  }
}
