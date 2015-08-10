package com.neverwinterdp.scribengin.dataflow.worker;

import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.task.TaskContext;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowTask;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskDescriptor;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.yara.MetricRegistry;
import com.neverwinterdp.yara.Timer;

public class DataflowTaskDedicatedExecutor extends DataflowTaskExecutor {
  private boolean                     kill           = false;
  private DataflowTask                dataflowTask   = null;
  private DataflowTaskExecutorThread  executorThread;
  
  public DataflowTaskDedicatedExecutor(DataflowTaskExecutorService service, DataflowTaskExecutorDescriptor descriptor) throws RegistryException {
    super(service, descriptor);
  }
  
  public void start() {
    executorThread = new DataflowTaskExecutorThread() ;
    executorThread.start();
    
  }
  
  public void interrupt() throws Exception {
    if(isAlive()) {
      if(dataflowTask != null) dataflowTask.interrupt();
    }
  }
  
  public boolean isAlive() {
    if(executorThread == null) return false;
    return executorThread.isAlive();
  }
  
  /**
   * This method is used to simulate the failure
   * @throws Exception
   */
  public void simulateKill() throws Exception {
    kill = true;
    if(executorThread != null && executorThread.isAlive()) executorThread.interrupt();
  }
  
  public class DataflowTaskExecutorThread extends Thread {
    
    public void run() {
      executorDescriptor.setStatus(DataflowTaskExecutorDescriptor.Status.RUNNING);
      MetricRegistry metricRegistry = executorService.getMetricRegistry();
      DataflowRegistry dflRegistry = executorService.getDataflowRegistry();
      
      try {
        Timer.Context dataflowTaskTimerGrabCtx = metricRegistry.getTimer("dataflow-task.timer.grab").time() ;
        TaskContext<DataflowTaskDescriptor> taskContext = dflRegistry.dataflowTaskAssign(executorService.getVMDescriptor());
        dataflowTaskTimerGrabCtx.stop();

        if(taskContext == null) {
          doExit(DataflowTaskExecutorDescriptor.Status.TERMINATED);
          return;
        }

        Timer.Context dataflowTaskTimerProcessCtx = metricRegistry.getTimer("dataflow-task.timer.process").time() ;
        executorDescriptor.addAssignedTask(taskContext.getTaskTransactionId().getTaskId());
        dflRegistry.
          getWorkerRegistry().
          updateWorkerTaskExecutor(executorService.getVMDescriptor(), executorDescriptor);
        dataflowTask = new DataflowTask(executorService, taskContext);
        dataflowTask.init();
        dataflowTask.execute();
        dataflowTask.finish();
        dataflowTaskTimerProcessCtx.stop();
        if(dataflowTask.isIterrupted()) {
          doExit(DataflowTaskExecutorDescriptor.Status.TERMINATED_WITH_INTERRUPT);
        } else {
          doExit(DataflowTaskExecutorDescriptor.Status.TERMINATED);
        }
      } catch (Throwable e) {
        executorService.getLogger().error("DataflowTaskExecutor Error", e);
        doExit(DataflowTaskExecutorDescriptor.Status.TERMINATED_WITH_ERROR);
      } finally {
        notifyExecutorTermination();
      }
    }
    
    void doExit(DataflowTaskExecutorDescriptor.Status status) {
      if(kill) return ;
      try {
        executorDescriptor.setStatus(status);
        DataflowRegistry dataflowRegistry = executorService.getDataflowRegistry();
        dataflowRegistry.
          getWorkerRegistry().
          updateWorkerTaskExecutor(executorService.getVMDescriptor(), executorDescriptor);
      } catch(Exception ex) {
        executorService.getLogger().error("DataflowTaskExecutor Fail To Updat Status", ex);
      }
    }
  }
}