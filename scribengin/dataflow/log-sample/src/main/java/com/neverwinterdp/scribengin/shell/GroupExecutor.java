package com.neverwinterdp.scribengin.shell;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class GroupExecutor {
  private String name ;
  private List<Executor> executors = new ArrayList<>();
  private long maxRunTime  = 10000;
  private long waitForReady = 3000;
  private  ExecutorService service ;
  
  public GroupExecutor(String name) {
    this.name = name; 
  }
  
  public String getName() { return this.name ; }
  
  public void add(Executor executor) {
    executors.add(executor) ;
  }
  
  public GroupExecutor withMaxRuntime(long maxRuntime) {
    this.maxRunTime = maxRuntime;
    return this ;
  }
  
  public GroupExecutor withWaitForReady(long time) {
    this.waitForReady = time;
    return this ;
  }
  
  public void execute() {
    service = Executors.newFixedThreadPool(executors.size());
    for(int i = 0; i < executors.size(); i++) {
      service.execute(executors.get(i));
    }
    service.shutdown();
  }
  
  public void waitForReady() throws Exception {
    if(waitForReady > 0) {
      Thread.sleep(waitForReady);
    }
  }
  
  public void awaitTermination() throws Exception {
    boolean terminated = service.awaitTermination(maxRunTime, TimeUnit.MILLISECONDS);
    if(!terminated) {
      throw new Exception("Fail to wait for termination for the group executor " + name + " in " + maxRunTime + "ms") ;
    }
  }
}
