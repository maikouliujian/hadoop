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

package org.apache.hadoop.yarn.event;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import com.google.common.annotations.VisibleForTesting;

/**
 * Dispatches {@link Event}s in a separate thread. Currently only single thread
 * does that. Potentially there could be multiple channels for each event type
 * class and a thread pool can be used to dispatch the events.
 */
@SuppressWarnings("rawtypes")
@Public
@Evolving
public class AsyncDispatcher extends AbstractService implements Dispatcher {

  private static final Log LOG = LogFactory.getLog(AsyncDispatcher.class);
  /*************************************************
   * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
   *  注释： 存放封装了各类事件的 Event 对象的阻塞队列
   */
  private final BlockingQueue<Event> eventQueue;
  private volatile int lastEventQueueSizeLogged = 0;
  private volatile int lastEventDetailsQueueSizeLogged = 0;
  private volatile boolean stopped = false;

  //Configuration for control the details queue event printing.
  private int detailsInterval;
  private boolean printTrigger = false;
  // TODO 注释： 配置标志，用于启用/禁用在停止功能上排空调度程序的事件。
  // Configuration flag for enabling/disabling draining dispatcher's events on
  // stop functionality.
  private volatile boolean drainEventsOnStop = false;

  // Indicates all the remaining dispatcher's events on stop have been drained
  // and processed.
  // Race condition happens if dispatcher thread sets drained to true between
  // handler setting drained to false and enqueueing event. YARN-3878 decided
  // to ignore it because of its tiny impact. Also see YARN-5436.
  // TODO 注释： 表示所有剩余的已停止调度事件已经被处理
  private volatile boolean drained = true;
  // TODO 注释： 等待排空的对象（锁）
  private final Object waitForDrained = new Object();

  // For drainEventsOnStop enabled only, block newly coming events into the
  // queue while stopping.
  // TODO 注释： 仅当drainEventsOnStop为true时可用，如果为true表示在停止时会阻塞新来的事件加入队列
  private volatile boolean blockNewEvents = false;
  /*************************************************
   * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
   *  注释： GenericEventHandler 负责往 eventQueue 里放入 event
   */
  private final EventHandler<Event> handlerInstance = new GenericEventHandler();
  /*************************************************
   * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
   *  注释： 事件处理线程 = 处理所有事件的线程
   */
  private Thread eventHandlingThread;
  // TODO 注释： 存放不同事件对应的事件处理器的映射表
  protected final Map<Class<? extends Enum>, EventHandler> eventDispatchers;
  private boolean exitOnDispatchException = true;

  /**
   * The thread name for dispatcher.
   */
  private String dispatcherThreadName = "AsyncDispatcher event handler";

  public AsyncDispatcher() {
    this(new LinkedBlockingQueue<Event>());
  }

  public AsyncDispatcher(BlockingQueue<Event> eventQueue) {
    super("Dispatcher");
    this.eventQueue = eventQueue;
    this.eventDispatchers = new HashMap<Class<? extends Enum>, EventHandler>();
  }

  /**
   * Set a name for this dispatcher thread.
   * @param dispatcherName name of the dispatcher thread
   */
  public AsyncDispatcher(String dispatcherName) {
    this();
    dispatcherThreadName = dispatcherName;
  }

  Runnable createThread() {
    return new Runnable() {
      @Override
      public void run() {
        while (!stopped && !Thread.currentThread().isInterrupted()) {
          //todo 【drained:排空】
          drained = eventQueue.isEmpty();
          // blockNewEvents is only set when dispatcher is draining to stop,
          // adding this check is to avoid the overhead of acquiring the lock
          // and calling notify every time in the normal run of the loop.
          // TODO 注释： blockNewEvents 仅在 dispatcher 排空、停止时设置为 true，
          // TODO 注释： 所以此检查是为了避免每次在循环的正常运行中获取锁和调用 notify 的开销。
          if (blockNewEvents) {
            synchronized (waitForDrained) {
              if (drained) {
                waitForDrained.notify();
              }
            }
          }
          Event event;
          try {
            event = eventQueue.take();
          } catch(InterruptedException ie) {
            if (!stopped) {
              LOG.warn("AsyncDispatcher thread interrupted", ie);
            }
            return;
          }
          if (event != null) {
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 分发处理该有效事件 Event
             *  从事先注册的 Event + EventHandler 中，根据 Event 找到对应的 EventHandler
             */
            dispatch(event);
            if (printTrigger) {
              //Log the latest dispatch event type
              // may cause the too many events queued
              LOG.info("Latest dispatch event type: " + event.getType());
              printTrigger = false;
            }
          }
        }
      }
    };
  }

  @VisibleForTesting
  public void disableExitOnDispatchException() {
    exitOnDispatchException = false;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception{
    super.serviceInit(conf);
    this.detailsInterval = getConfig().getInt(YarnConfiguration.
                    YARN_DISPATCHER_PRINT_EVENTS_INFO_THRESHOLD,
            YarnConfiguration.
                    DEFAULT_YARN_DISPATCHER_PRINT_EVENTS_INFO_THRESHOLD);
  }

  @Override
  protected void serviceStart() throws Exception {
    //start all the components
    super.serviceStart();
    eventHandlingThread = new Thread(createThread());
    eventHandlingThread.setName(dispatcherThreadName);
    eventHandlingThread.start();
  }

  public void setDrainEventsOnStop() {
    drainEventsOnStop = true;
  }

  @Override
  protected void serviceStop() throws Exception {
    if (drainEventsOnStop) {
      blockNewEvents = true;
      LOG.info("AsyncDispatcher is draining to stop, ignoring any new events.");
      long endTime = System.currentTimeMillis() + getConfig()
          .getLong(YarnConfiguration.DISPATCHER_DRAIN_EVENTS_TIMEOUT,
              YarnConfiguration.DEFAULT_DISPATCHER_DRAIN_EVENTS_TIMEOUT);

      synchronized (waitForDrained) {
        while (!isDrained() && eventHandlingThread != null
            && eventHandlingThread.isAlive()
            && System.currentTimeMillis() < endTime) {
          waitForDrained.wait(100);
          LOG.info("Waiting for AsyncDispatcher to drain. Thread state is :" +
              eventHandlingThread.getState());
        }
      }
    }
    stopped = true;
    if (eventHandlingThread != null) {
      eventHandlingThread.interrupt();
      try {
        eventHandlingThread.join();
      } catch (InterruptedException ie) {
        LOG.warn("Interrupted Exception while stopping", ie);
      }
    }

    // stop all the components
    super.serviceStop();
  }

  @SuppressWarnings("unchecked")
  protected void dispatch(Event event) {
    //all events go thru this loop
    if (LOG.isDebugEnabled()) {
      LOG.debug("Dispatching the event " + event.getClass().getName() + "."
          + event.toString());
    }
    //todo 获取事件类型
    Class<? extends Enum> type = event.getType().getDeclaringClass();

    try{
      EventHandler handler = eventDispatchers.get(type);
      if(handler != null) {
        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 根据事件类型，调用对应的 Handler 执行处理
         */
        handler.handle(event);
      } else {
        throw new Exception("No handler for registered for " + type);
      }
    } catch (Throwable t) {
      //TODO Maybe log the state of the queue
      LOG.fatal("Error in dispatcher thread", t);
      // If serviceStop is called, we should exit this thread gracefully.
      if (exitOnDispatchException
          && (ShutdownHookManager.get().isShutdownInProgress()) == false
          && stopped == false) {
        stopped = true;
        Thread shutDownThread = new Thread(createShutDownThread());
        shutDownThread.setName("AsyncDispatcher ShutDown handler");
        shutDownThread.start();
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void register(Class<? extends Enum> eventType,
      EventHandler handler) {
    /* check to see if we have a listener registered */
    EventHandler<Event> registeredHandler = (EventHandler<Event>)
    eventDispatchers.get(eventType);
    LOG.info("Registering " + eventType + " for " + handler.getClass());
    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 进行 EventType 和 EventHandler 之间的映射注册！
     */
    if (registeredHandler == null) {
      eventDispatchers.put(eventType, handler);
      // TODO 注释： 如果不是 MultiListenerHandler 类型，就将老的取出和参数 handler 一起加到新建的 multiHandler
    } else if (!(registeredHandler instanceof MultiListenerHandler)){
      /* for multiple listeners of an event add the multiple listener handler */
      MultiListenerHandler multiHandler = new MultiListenerHandler();
      multiHandler.addHandler(registeredHandler);
      multiHandler.addHandler(handler);
      eventDispatchers.put(eventType, multiHandler);
    } else {
      // TODO 注释： 已经是MultiListenerHandler，直接添加
      /* already a multilistener, just add to it */
      MultiListenerHandler multiHandler
      = (MultiListenerHandler) registeredHandler;
      multiHandler.addHandler(handler);
    }
  }

  @Override
  public EventHandler<Event> getEventHandler() {
    return handlerInstance;
  }

  class GenericEventHandler implements EventHandler<Event> {
    private void printEventQueueDetails(BlockingQueue<Event> queue) {
      Map<Enum, Long> counterMap = eventQueue.stream().
              collect(Collectors.
                      groupingBy(e -> e.getType(), Collectors.counting())
              );
      for (Map.Entry<Enum, Long> entry : counterMap.entrySet()) {
        long num = entry.getValue();
        LOG.info("Event type: " + entry.getKey()
                + ", Event record counter: " + num);
      }
    }
    public void handle(Event event) {
      if (blockNewEvents) {
        return;
      }
      drained = false;

      /* all this method does is enqueue all the events onto the queue */
      int qSize = eventQueue.size();
      if (qSize != 0 && qSize % 1000 == 0
          && lastEventQueueSizeLogged != qSize) {
        lastEventQueueSizeLogged = qSize;
        LOG.info("Size of event-queue is " + qSize);
      }
      if (qSize != 0 && qSize % detailsInterval == 0
              && lastEventDetailsQueueSizeLogged != qSize) {
        lastEventDetailsQueueSizeLogged = qSize;
        printEventQueueDetails(eventQueue);
        printTrigger = true;
      }
      int remCapacity = eventQueue.remainingCapacity();
      if (remCapacity < 1000) {
        LOG.warn("Very low remaining capacity in the event-queue: "
            + remCapacity);
      }
      try {
        eventQueue.put(event);
      } catch (InterruptedException e) {
        if (!stopped) {
          LOG.warn("AsyncDispatcher thread interrupted", e);
        }
        // Need to reset drained flag to true if event queue is empty,
        // otherwise dispatcher will hang on stop.
        drained = eventQueue.isEmpty();
        throw new YarnRuntimeException(e);
      }
    };
  }

  /**
   * Multiplexing an event. Sending it to different handlers that
   * are interested in the event.
   * @param <T> the type of event these multiple handlers are interested in.
   */
  static class MultiListenerHandler implements EventHandler<Event> {
    List<EventHandler<Event>> listofHandlers;

    public MultiListenerHandler() {
      listofHandlers = new ArrayList<EventHandler<Event>>();
    }

    @Override
    public void handle(Event event) {
      for (EventHandler<Event> handler: listofHandlers) {
        handler.handle(event);
      }
    }

    void addHandler(EventHandler<Event> handler) {
      listofHandlers.add(handler);
    }

  }

  Runnable createShutDownThread() {
    return new Runnable() {
      @Override
      public void run() {
        LOG.info("Exiting, bbye..");
        System.exit(-1);
      }
    };
  }

  @VisibleForTesting
  protected boolean isEventThreadWaiting() {
    return eventHandlingThread.getState() == Thread.State.WAITING;
  }

  protected boolean isDrained() {
    return drained;
  }

  protected boolean isStopped() {
    return stopped;
  }
}
