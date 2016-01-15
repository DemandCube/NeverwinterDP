package com.neverwinterdp.scribengin.dataflow.runtime.master;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class DataflowActivityExecutor {
  private BlockingQueue<DataflowMasterActivity> activities = new LinkedBlockingQueue<>();
  private ExecutorThread executorThread = new ExecutorThread();
  
  public DataflowActivityExecutor() {
    executorThread = new ExecutorThread();
    executorThread.start();
  }
  
  public void add(DataflowMasterActivity activity) {
    activities.add(activity);
  }
  
  public void onDestroy() {
    executorThread.terminate = true;
    executorThread.interrupt();
  }
  
  public class ExecutorThread extends Thread {
    private boolean terminate = false;
    public void run() {
      DataflowMasterActivity activity = null;
      try {
        while((activity = activities.take()) != null) {
          if(terminate) return;
          try {
            activity.execute();
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      } catch (InterruptedException e) {
      }
    }
  }
}
