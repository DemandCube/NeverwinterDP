package com.neverwinterdp.scribengin.dataflow.worker;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class DataflowTaskExecutors {
  private List<DataflowTaskExecutor> taskExecutors = new ArrayList<>();
  private CountDownLatch terminatedLatch ; 
  
  public int count() { return taskExecutors.size() ; }

  public int countAlive() {
    int count = 0 ;
    for(DataflowTaskExecutor sel : taskExecutors) {
      if(sel.isAlive()) count++ ;
    }
    return count;
  }
  
  public DataflowTaskExecutor getExecutor(int idx) { return taskExecutors.get(idx); }
  
  public void add(DataflowTaskExecutor executor) {
    taskExecutors.add(executor) ;
  }

  public List<DataflowTaskExecutor> getTaskExecutors() { return taskExecutors; }

  public void setTaskExecutors(List<DataflowTaskExecutor> taskExecutors) {
    this.taskExecutors = taskExecutors;
  }
  
  void interrupt() throws Exception {
    for(DataflowTaskExecutor sel : taskExecutors) {
      if(sel.isAlive()) sel.interrupt();
    }
  }
  
  public void start() throws Exception {
    terminatedLatch = new CountDownLatch(taskExecutors.size()) ; 
    for(int i = 0; i < taskExecutors.size(); i++) {
      DataflowTaskExecutor executor = taskExecutors.get(i);
      executor.start();
    }
  }

  public void simulateKill() throws Exception {
    for(DataflowTaskExecutor sel : taskExecutors) {
      if(sel.isAlive()) sel.simulateKill();
    }
  }
  
  public void notifyExecutorTermination() {
    terminatedLatch.countDown();
  }
  
  public void waitForTermination() throws InterruptedException {
    if(terminatedLatch == null) return ;
    terminatedLatch.await();
    terminatedLatch = null ;
  }
}
