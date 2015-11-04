package com.neverwinterdp.registry.task.dedicated;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TaskExecutorService<T> {
  private List<TaskExecutor<T>> taskExecutors = new ArrayList<TaskExecutor<T>>();
  private List<TaskExecutorThread> taskExecutorsThreads = new ArrayList<>();
  
  private TaskSlotTimerThread taskSlotTimer ;
  
  public void add(TaskExecutor<T> executor) {
    taskExecutors.add(executor);
  }
  
  public void startExecutors() throws InterruptedException {
    startExecutors(-1);
  }
  
  public void startExecutors(long breakIn) throws InterruptedException {
    for(int i = 0; i < taskExecutors.size(); i++) {
      TaskExecutor<T> executor = taskExecutors.get(i) ;
      TaskExecutorThread executorThread = new TaskExecutorThread(executor);
      taskExecutorsThreads.add(executorThread);
      executorThread.start();
      if(breakIn > 0) Thread.sleep(breakIn);
    }
    taskSlotTimer = new TaskSlotTimerThread();
    taskSlotTimer.start();
  }
  
  
  
  public void shutdown() throws InterruptedException {
    if(taskSlotTimer  != null) {
      taskSlotTimer.interrupt();
    }
    
    if(taskExecutors.size() > 0) {
      for(TaskExecutor<T> executor : taskExecutors) {
        executor.shutdown();
      }
      awaitTermination(30, TimeUnit.SECONDS);
    }
  }
  
  synchronized public void awaitTermination(long maxWaitTime, TimeUnit unit) throws InterruptedException {
    maxWaitTime = unit.toMillis(maxWaitTime);
    long startTime = System.currentTimeMillis();
    while(taskExecutorsThreads.size() > 0) {
      long remainWaitTime = maxWaitTime - (System.currentTimeMillis() - startTime);
      if(remainWaitTime <= 0) break;
      wait(remainWaitTime);
      Iterator<TaskExecutorThread> i = taskExecutorsThreads.iterator();
      while(i.hasNext()) {
        if(!i.next().isAlive()) i.remove();
      }
    }
    taskSlotTimer.interrupt();
  }
  
  synchronized void notifyThreadTermination() {
    notifyAll();
  }
  
  
  public void simulateKill() {
    taskSlotTimer.interrupt();
    Iterator<TaskExecutorThread> i = taskExecutorsThreads.iterator();
    while(i.hasNext()) {
      i.next().interrupt();
    }
  }
  
  public class TaskSlotTimerThread extends Thread {
    public void run() {
      try {
        while(true) {
          for(int i = 0; i < taskExecutors.size(); i++) {
            Thread.sleep(3000);
            taskExecutors.get(i).onSwitchTaskSlot();
          }
        }
      } catch (InterruptedException e) {
      }
    }
  }
  
  public class TaskExecutorThread extends Thread {
    TaskExecutor<T> executor;
    
    TaskExecutorThread(TaskExecutor<T> executor) {
      this.executor = executor;
    }
    
    public void run() {
      executor.run();
      notifyThreadTermination();
    }
    
    public void shutdown() {
    }
  }
}
