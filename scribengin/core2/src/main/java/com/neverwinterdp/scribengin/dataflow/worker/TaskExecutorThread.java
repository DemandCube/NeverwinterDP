package com.neverwinterdp.scribengin.dataflow.worker;

public class TaskExecutorThread extends Thread {
  private long    taskStartTime;
  private long    taskSwitchPeriod = 1000;
  private boolean taskSwitch       = false;

  public void onTaskSwitch(long currentTime) {
    if(taskSwitch) return; //already set switch flag
    long runtime = currentTime  - taskStartTime ;
    if(runtime < 0) runtime  = 0;
    if(runtime > taskSwitchPeriod) {
      taskSwitch = true;
    }
  }

  
  public void run() {
    try {
      taskStartTime = System.currentTimeMillis();
      int count = 0;
      while(count < 5) {
        System.out.println("run task: " + (System.currentTimeMillis() - taskStartTime) + "ms");
        if(taskSwitch) {
          taskSwitch = false;
          taskStartTime = System.currentTimeMillis();
          System.out.println(" task switch");
        }
        count++;
        Thread.sleep(500);
      }
    } catch(InterruptedException ex) {
    }
  }
}
