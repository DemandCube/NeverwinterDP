package com.neverwinterdp.scribengin.dataflow.worker;

import com.neverwinterdp.registry.task.TaskContext;
import com.neverwinterdp.scribengin.dataflow.operator.OperatorTask;
import com.neverwinterdp.scribengin.dataflow.operator.OperatorTaskConfig;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.yara.MetricRegistry;
import com.neverwinterdp.yara.Timer;

public class TaskExecutorThread extends Thread {
  private WorkerService          workerService;
  private TaskExecutorDescriptor taskExecutorDescriptor;
  
  private long         taskStartTime;
  private long         taskSwitchPeriod = 5000;
  private boolean      taskSwitch       = false;
  private boolean      kill = false;
  private OperatorTask operatorTask;

  public TaskExecutorThread(WorkerService workerService, TaskExecutorDescriptor descriptor) {
    this.workerService = workerService;
    this.taskExecutorDescriptor = descriptor;
  }
  
  public void onTaskSwitch(long currentTime) {
    if(taskSwitch) return; //already set switch flag
    long runtime = currentTime  - taskStartTime ;
    if(runtime < 0) runtime  = 0;
    if(runtime > taskSwitchPeriod) {
      taskSwitch = true;
    }
  }

  
//  public void run() {
//    try {
//      taskStartTime = System.currentTimeMillis();
//      int count = 0;
//      while(count < 5) {
//        System.out.println("run task: " + (System.currentTimeMillis() - taskStartTime) + "ms");
//        if(taskSwitch) {
//          taskSwitch = false;
//          taskStartTime = System.currentTimeMillis();
//          System.out.println(" task switch");
//        }
//        count++;
//        Thread.sleep(500);
//      }
//    } catch(InterruptedException ex) {
//    }
//  }
  
  public void run() {
    taskExecutorDescriptor.setStatus(TaskExecutorDescriptor.Status.RUNNING);
    MetricRegistry metricRegistry = workerService.getMetricRegistry();
    DataflowRegistry dflRegistry  = workerService.getDataflowRegistry();
    
    try {
      Timer.Context dataflowTaskTimerGrabCtx = metricRegistry.getTimer("dataflow-task.timer.grab").time() ;
      TaskContext<OperatorTaskConfig> taskContext = 
          dflRegistry.getTaskRegistry().take(workerService.getVMDescriptor());
      dataflowTaskTimerGrabCtx.stop();

      if(taskContext == null) {
        doExit(TaskExecutorDescriptor.Status.TERMINATED);
        return;
      }

      Timer.Context dataflowTaskTimerProcessCtx = metricRegistry.getTimer("dataflow-task.timer.process").time() ;
      taskExecutorDescriptor.addAssignedTask(taskContext.getTaskTransactionId().getTaskId());
      dflRegistry.
        getWorkerRegistry().
        updateWorkerTaskExecutor(workerService.getVMDescriptor(), taskExecutorDescriptor);
      operatorTask = new OperatorTask(workerService, taskContext);
      operatorTask.init();
      operatorTask.execute();
      operatorTask.finish();
      dataflowTaskTimerProcessCtx.stop();
      if(operatorTask.isIterrupted()) {
        doExit(TaskExecutorDescriptor.Status.TERMINATED_WITH_INTERRUPT);
      } else {
        doExit(TaskExecutorDescriptor.Status.TERMINATED);
      }
    } catch (Throwable e) {
      workerService.getLogger().error("DataflowTaskExecutor Error", e);
      doExit(TaskExecutorDescriptor.Status.TERMINATED_WITH_ERROR);
    } finally {
      notifyExecutorTermination();
    }
  }
  
  void doExit(TaskExecutorDescriptor.Status status) {
    if(kill) return ;
    try {
      DataflowRegistry dflRegistry  = workerService.getDataflowRegistry();
      taskExecutorDescriptor.setStatus(status);
      dflRegistry.
        getWorkerRegistry().
        updateWorkerTaskExecutor(workerService.getVMDescriptor(), taskExecutorDescriptor);
    } catch(Exception ex) {
      workerService.getLogger().error("DataflowTaskExecutor Fail To Updat Status", ex);
    }
  }
  
  protected void notifyExecutorTermination() {
    //TODO: review and implement
  }
  
}
