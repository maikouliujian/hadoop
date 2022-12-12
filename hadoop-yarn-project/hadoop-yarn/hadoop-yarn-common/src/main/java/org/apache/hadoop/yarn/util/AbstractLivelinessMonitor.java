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

package org.apache.hadoop.yarn.util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.service.AbstractService;

/**
 * A simple liveliness monitor with which clients can register, trust the
 * component to monitor liveliness, get a call-back on expiry and then finally
 * unregister.
 */
@Public
@Evolving
public abstract class AbstractLivelinessMonitor<O> extends AbstractService {
  // TODO 注释： 工作机制总结：每隔一段时间，对所有存活节点，都做一次死活判断，必然通过一个定时线程来实现
  // TODO 注释： 包含以下必要的信息
  // TODO 注释： 一个线程 checkerThread
  // TODO 注释： 一个注册表/心跳表 Map<O, Long> running
  // TODO 注释： 线程的间隔工作时间 monitorInterval
  // TODO 注释： 判断组件是否死亡的一个超时时间 expireInterval
  private static final Log LOG = LogFactory.getLog(AbstractLivelinessMonitor.class);

  //thread which runs periodically to see the last time since a heartbeat is
  //received.
  // TODO 注释： 检测线程
  private Thread checkerThread;
  private volatile boolean stopped;
  // TODO 注释： 默认过期时间
  public static final int DEFAULT_EXPIRE = 5*60*1000;//5 mins
  private long expireInterval = DEFAULT_EXPIRE;
  // TODO 注释： 检测时间间隔是 过期时间的 1/3
  private long monitorInterval = expireInterval / 3;
  private volatile boolean resetTimerOnStart = true;

  private final Clock clock;
  /*************************************************
   * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
   *  注释： 所有成功注册的 组件 的集合
   *  1、key = 组件ID
   *  2、value = 时间戳 = 最近一次成功心跳的时间，如果没有心跳，那就是注册
   *  这些个组件，可以是 NodeManager，可以是 ApplicationMaster，可以是 RMApp， 可以是 Container
   */
  private Map<O, Long> running = new HashMap<O, Long>();

  public AbstractLivelinessMonitor(String name, Clock clock) {
    super(name);
    this.clock = clock;
  }

  public AbstractLivelinessMonitor(String name) {
    this(name, new MonotonicClock());
  }

  @Override
  protected void serviceStart() throws Exception {
    assert !stopped : "starting when already stopped";
    resetTimer();
    checkerThread = new Thread(new PingChecker());
    checkerThread.setName("Ping Checker");
    checkerThread.start();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    stopped = true;
    if (checkerThread != null) {
      checkerThread.interrupt();
    }
    super.serviceStop();
  }

  protected abstract void expire(O ob);

  protected void setExpireInterval(int expireInterval) {
    this.expireInterval = expireInterval;
  }

  protected long getExpireInterval(O o) {
    // by-default return for all the registered object interval.
    return this.expireInterval;
  }

  protected void setMonitorInterval(long monitorInterval) {
    this.monitorInterval = monitorInterval;
  }

  public synchronized void receivedPing(O ob) {
    //only put for the registered objects
    if (running.containsKey(ob)) {
      running.put(ob, clock.getTime());
    }
  }

  public synchronized void register(O ob) {
    // TODO 注释： 注册
    // TODO 注释： key  = 组件ID
    // TODO 注释： value  = 当前时间
    // TODO 注释： 如果 上一次心跳时间 value + 最大心跳超时时间 5min < 当前时间，证明心跳超时了
    register(ob, clock.getTime());
  }

  public synchronized void register(O ob, long expireTime) {
    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 注册一个 管理对象的 超时时间到  running 这个 map 中
     */
    running.put(ob, expireTime);
  }

  public synchronized void unregister(O ob) {
    running.remove(ob);
  }

  public synchronized void resetTimer() {
    if (resetTimerOnStart) {
      long time = clock.getTime();
      for (O ob : running.keySet()) {
        running.put(ob, time);
      }
    }
  }

  protected void setResetTimeOnStart(boolean resetTimeOnStart) {
    this.resetTimerOnStart = resetTimeOnStart;
  }

  private class PingChecker implements Runnable {

    @Override
    public void run() {
      while (!stopped && !Thread.currentThread().isInterrupted()) {
        synchronized (AbstractLivelinessMonitor.this) {
          Iterator<Map.Entry<O, Long>> iterator = running.entrySet().iterator();

          // avoid calculating current time everytime in loop
          long currentTime = clock.getTime();

          while (iterator.hasNext()) {
            Map.Entry<O, Long> entry = iterator.next();
            O key = entry.getKey();
            long interval = getExpireInterval(key);
            if (currentTime > entry.getValue() + interval) {
              iterator.remove();
              expire(key);
              LOG.info("Expired:" + entry.getKey().toString()
                  + " Timed out after " + interval / 1000 + " secs");
            }
          }
        }
        try {
          Thread.sleep(monitorInterval);
        } catch (InterruptedException e) {
          LOG.info(getName() + " thread interrupted");
          break;
        }
      }
    }
  }

}
