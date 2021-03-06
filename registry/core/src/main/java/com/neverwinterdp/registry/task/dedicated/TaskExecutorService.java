package com.neverwinterdp.registry.task.dedicated;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TaskExecutorService<T> {
  private List<TaskExecutor<T>>    taskExecutors = new ArrayList<TaskExecutor<T>>();
  private List<TaskExecutorThread> taskExecutorsThreads = new ArrayList<>();
  
  private TaskSlotTimerThread taskSlotTimer ;
  
  public  List<TaskExecutor<T>> getTaskExecutors() { return taskExecutors; }
  
  public void add(TaskExecutor<T> executor) {
    taskExecutors.add(executor);
  }
  
  public void broadcast(TaskExecutorEvent event) throws Exception {
    for(int i = 0; i < taskExecutors.size(); i++) {
      taskExecutors.get(i).broadcast(event);
    }
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
    }
  }
  
  synchronized public void awaitTermination() throws InterruptedException {
    while(taskExecutorsThreads.size() > 0) {
      wait(3000);
      Iterator<TaskExecutorThread> i = taskExecutorsThreads.iterator();
      while(i.hasNext()) {
        if(!i.next().isAlive()) {
          i.remove();
        }
      }
    }
    taskSlotTimer.interrupt();
  }
  
  synchronized void notifyThreadTermination() {
    notifyAll();
  }
  
  
  public void simulateKill() throws InterruptedException {
    taskSlotTimer.interrupt();
    for(TaskExecutor<T> executor : taskExecutors) {
      executor.simulateKill();
    }
  }
  
  public class TaskSlotTimerThread extends Thread {
    public void run() {
      try {
        while(true) {
          Thread.sleep(1000);
          long time = System.currentTimeMillis();
          for(int i = 0; i < taskExecutors.size(); i++) {
            taskExecutors.get(i).setTickTime(time);;
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
