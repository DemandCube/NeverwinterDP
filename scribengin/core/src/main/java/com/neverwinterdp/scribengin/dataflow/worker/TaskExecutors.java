package com.neverwinterdp.scribengin.dataflow.worker;

import java.util.ArrayList;
import java.util.List;

import com.neverwinterdp.scribengin.dataflow.config.DataflowConfig;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;

public class TaskExecutors {
  private WorkerService    workerService;
  private DataflowRegistry dflRegistry ;
  private List<TaskExecutorThread> taskExecutorThreads;
  private TaskExecutorMonitorThread monitorThread;
  private boolean simulateKill = false;
  
  public TaskExecutors(WorkerService workerService) {
    this.workerService = workerService;
    this.dflRegistry = workerService.getDataflowRegistry();
  }
  
  public boolean getSimulateKill() { return simulateKill; }
  
  synchronized public void start() throws Exception {
    DataflowConfig dflConfig = dflRegistry.getConfigRegistry().getDataflowConfig();
    int numOfExecutor = dflConfig.getWorker().getNumOfExecutor();
    taskExecutorThreads = new ArrayList<>();
    for(int i = 0; i < numOfExecutor; i++) {
      TaskExecutorDescriptor descriptor = new TaskExecutorDescriptor("executor-" + (i + 1));
      TaskExecutorThread thread = new TaskExecutorThread(workerService, descriptor);
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
      if(simulateKill) return;
    }
  }
  
  public void simulateKill() throws Exception {
    simulateKill = true;
    for(int i = 0; i < taskExecutorThreads.size(); i++) {
      TaskExecutorThread thread = taskExecutorThreads.get(i);
      if(thread.isAlive())thread.simulateKill();
    }
    if(monitorThread.isAlive()) {
      monitorThread.interrupt();
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
