package com.neverwinterdp.scribengin.dataflow.worker;

import java.util.ArrayList;
import java.util.List;

import com.neverwinterdp.scribengin.dataflow.config.DataflowConfig;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;

public class TaskExecutorService {
  private DataflowRegistry dflRegistry ;
  private List<TaskExecutorThread> taskExecutorThreads;
  private TaskExecutorMonitorThread monitorThread;
  private long taskSwitchingPeriod ;
  
  public TaskExecutorService(DataflowRegistry dflRegistry) {
    this.dflRegistry = dflRegistry;
  }
  
  synchronized public void start() throws Exception {
    DataflowConfig dflConfig = dflRegistry.getConfigRegistry().getDataflowConfig();
    taskSwitchingPeriod = dflConfig.getWorker().getTaskSwitchingPeriod();
    int numOfExecutor = dflConfig.getWorker().getNumOfExecutor();
    taskExecutorThreads = new ArrayList<>();
    for(int i = 0; i < numOfExecutor; i++) {
      TaskExecutorThread thread = new TaskExecutorThread();
      thread.start();
      taskExecutorThreads.add(thread);
    }
    monitorThread = new TaskExecutorMonitorThread();
    monitorThread.start();
  }
  
  synchronized public void stop() throws Exception {
    if(monitorThread == null || !monitorThread.isAlive()) return ;
    monitorThread.interrupt();
    for(int i = 0; i < taskExecutorThreads.size(); i++) {
      TaskExecutorThread thread = taskExecutorThreads.get(i);
      thread.interrupt();
    }
    taskExecutorThreads = null;
  }
  
  synchronized void waitForTermination() throws InterruptedException {
    if(monitorThread == null) return;
    monitorThread.waitForTermination();
  }
  
  public void monitor() {
    try {
      int runningExecutor = taskExecutorThreads.size();
      while(runningExecutor > 0) {
        Thread.sleep(500);
        long currentTime = System.currentTimeMillis();
        runningExecutor = 0;
        for(int i = 0; i < taskExecutorThreads.size(); i++) {
          TaskExecutorThread thread = taskExecutorThreads.get(i);
          if(!thread.isAlive()) continue;
          thread.onTaskSwitch(currentTime);
          runningExecutor++;
        }
      }
    } catch (InterruptedException e) {
    }
  }
  
  public class TaskExecutorMonitorThread extends Thread {
    public void run() {
      monitor();
      notifyTermination();
    }
    
    synchronized void waitForTermination() throws InterruptedException {
      if(!isAlive()) return;
      wait();
    }
    
    synchronized void notifyTermination() {
      notifyAll();
    }
  }
}
