package com.neverwinterdp.registry.task.dedicated;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TaskExecutorService<T> {
  private ExecutorService       execService;
  private List<TaskExecutor<T>> taskExecutors = new ArrayList<TaskExecutor<T>>();
  private TaskSlotTimerThread taskSlotTimer ;
  
  public void add(TaskExecutor<T> executor) {
    taskExecutors.add(executor);
  }
  
  public void startExecutors() {
    execService = Executors.newFixedThreadPool(taskExecutors.size());
    for(int i = 0; i < taskExecutors.size(); i++) {
      TaskExecutor<T> executor = taskExecutors.get(i) ;
      execService.submit(executor);
    }
    execService.shutdown();
    taskSlotTimer = new TaskSlotTimerThread();
    taskSlotTimer.start();
  }
  
  public void shutdown() {
    taskSlotTimer.interrupt();
    execService.shutdownNow();
  }
  
  public void awaitTermination(long maxWaitTime, TimeUnit unit) throws InterruptedException {
    execService.awaitTermination(maxWaitTime, unit);
    taskSlotTimer.interrupt();
  }
  
  
  public void simulateKill() {
    execService.shutdownNow();
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
}
