package com.neverwinterdp.scribengin.dataflow.worker;

import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.task.TaskContext;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.DataflowTask;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskDescriptor;
import com.neverwinterdp.yara.MetricRegistry;
import com.neverwinterdp.yara.Timer;

public class DataflowTaskExecutor {
  protected DataflowTaskExecutorDescriptor executorDescriptor;
  protected DataflowTaskExecutorService    executorService;
  
  private ExecutorManagerThread          executorManagerThread;
  private DataflowTaskExecutorThread     executorThread;
  private DataflowTask                   currentDataflowTask = null;
  private boolean                        interrupt           = false;
  private boolean                        kill                = false;

  public DataflowTaskExecutor(DataflowTaskExecutorService service, DataflowTaskExecutorDescriptor descriptor) throws RegistryException {
    executorDescriptor = descriptor;
    this.executorService = service;
    service.getDataflowRegistry().createWorkerTaskExecutor(service.getVMDescriptor(), descriptor);
  }
  
  public void start() {
    interrupt = false ;
    executorManagerThread = new ExecutorManagerThread();
    executorManagerThread.start();
  }
  
  public void interrupt() throws Exception {
    if(isAlive()) {
      interrupt = true ;
      if(currentDataflowTask != null) currentDataflowTask.interrupt();
    }
  }
  
  public boolean isAlive() {
    if(executorManagerThread == null) return false;
    return executorManagerThread.isAlive();
  }
  
  public void execute() { 
    executorDescriptor.setStatus(DataflowTaskExecutorDescriptor.Status.RUNNING);
    MetricRegistry metricRegistry = executorService.getMetricRegistry();
    DataflowRegistry dataflowRegistry = executorService.getDataflowRegistry();
    Timer dataflowTaskTimerGrab = metricRegistry.getTimer("dataflow-task.timer.grab") ;
    Timer dataflowTaskTimerProcess = metricRegistry.getTimer("dataflow-task.timer.process") ;
    try {
      DataflowDescriptor dflDescriptor = dataflowRegistry.getDataflowDescriptor(false);
      while(!interrupt) {
        Timer.Context dataflowTaskTimerGrabCtx = dataflowTaskTimerGrab.time() ;
        TaskContext<DataflowTaskDescriptor> taskContext = 
            dataflowRegistry.dataflowTaskAssign(executorService.getVMDescriptor());
        dataflowTaskTimerGrabCtx.stop();

        if(interrupt) {
          dataflowRegistry.dataflowTaskSuspend(taskContext);
          doExit(DataflowTaskExecutorDescriptor.Status.TERMINATED_WITH_INTERRUPT);
          return ;
        }
        
        if(taskContext == null) {
          doExit(DataflowTaskExecutorDescriptor.Status.TERMINATED);
          return;
        }
        
        Timer.Context dataflowTaskTimerProcessCtx = dataflowTaskTimerProcess.time() ;
        executorDescriptor.addAssignedTask(taskContext.getTaskTransactionId().getTaskId());
        dataflowRegistry.updateWorkerTaskExecutor(executorService.getVMDescriptor(), executorDescriptor);
        currentDataflowTask = new DataflowTask(executorService, taskContext);
        currentDataflowTask.init();
        executorThread = new DataflowTaskExecutorThread(currentDataflowTask);
        executorThread.start();
        executorThread.waitForTimeout(dflDescriptor.getTaskSwitchingPeriod());
        if(currentDataflowTask.isComplete()) {
          currentDataflowTask.finish();
        } else {
          currentDataflowTask.suspend();
        }
        dataflowTaskTimerProcessCtx.stop();
      }
      doExit(DataflowTaskExecutorDescriptor.Status.TERMINATED_WITH_INTERRUPT);
    } catch (Throwable e) {
      executorService.getLogger().error("DataflowTaskExecutor Error", e);
      doExit(DataflowTaskExecutorDescriptor.Status.TERMINATED_WITH_ERROR);
    }
  }

  void doExit(DataflowTaskExecutorDescriptor.Status status) {
    if(kill) return ;
    try {
      executorDescriptor.setStatus(status);
      DataflowRegistry dataflowRegistry = executorService.getDataflowRegistry();
      dataflowRegistry.updateWorkerTaskExecutor(executorService.getVMDescriptor(), executorDescriptor);
    } catch(Exception ex) {
      executorService.getLogger().error("DataflowTaskExecutor Fail To Updat Status", ex);
    }
  }
  
  /**
   * This method is used to simulate the failure
   * @throws Exception
   */
  public void simulateKill() throws Exception {
    kill = true;
    if(executorThread != null && executorThread.isAlive()) executorThread.interrupt();
    if(executorManagerThread != null && executorManagerThread.isAlive()) executorManagerThread.interrupt();
  }
  
  public class ExecutorManagerThread extends Thread {
    public void run() {
      execute();
    }
  }
  
  public class DataflowTaskExecutorThread extends Thread {
    DataflowTask  dataflowtask;
    private boolean terminated = false;
    
    public DataflowTaskExecutorThread(DataflowTask  dataflowtask) {
      this.dataflowtask = dataflowtask;
    }

    public void run() {
      try {
        dataflowtask.execute();
        notifyTermination();
      } catch (InterruptedException e) {
      }
    }
    
    synchronized public void notifyTermination() {
      terminated = true;
      notifyAll() ;
    }
    
    synchronized void waitForTimeout(long timeout) throws InterruptedException {
      if(timeout > 0) wait(timeout);
      else wait();
      if(!terminated) dataflowtask.interrupt();
      wait(3000);
    }
  }
}