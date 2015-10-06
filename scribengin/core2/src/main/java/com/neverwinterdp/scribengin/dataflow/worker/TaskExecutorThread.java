package com.neverwinterdp.scribengin.dataflow.worker;

import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.task.TaskContext;
import com.neverwinterdp.scribengin.dataflow.operator.OperatorTask;
import com.neverwinterdp.scribengin.dataflow.operator.OperatorTaskConfig;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowTaskRegistry;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.yara.MetricRegistry;
import com.neverwinterdp.yara.Timer;

public class TaskExecutorThread extends Thread {
  private WorkerService          workerService;
  private TaskExecutorDescriptor taskExecutorDescriptor;
  
  private long         taskStartTime;
  private long         taskSwitchPeriod = 5000;
  
  private boolean      kill = false;
  private boolean      interrupt = false;
  private OperatorTask operatorTask;

  public TaskExecutorThread(WorkerService workerService, TaskExecutorDescriptor descriptor) throws RegistryException {
    this.workerService = workerService;
    this.taskExecutorDescriptor = descriptor;
    this.taskSwitchPeriod = 
      workerService.getDataflowRegistry().getConfigRegistry().getDataflowConfig().getWorker().getTaskSwitchingPeriod();
    workerService.
      getDataflowRegistry().
      getWorkerRegistry().
      createWorkerTaskExecutor(workerService.getVMDescriptor(), descriptor);
  }
  
  public void onTaskSwitch(long currentTime) {
    if(operatorTask == null || operatorTask.isIterrupted()) return;
    long runtime = currentTime  - taskStartTime ;
    if(runtime < 0) runtime  = 0;
    if(runtime > taskSwitchPeriod) {
      operatorTask.interrupt();
    }
  }

  
  public void run() {
    taskExecutorDescriptor.setStatus(TaskExecutorDescriptor.Status.RUNNING);
    MetricRegistry metricRegistry = workerService.getMetricRegistry();
    DataflowRegistry dataflowRegistry = workerService.getDataflowRegistry();
    Timer dataflowTaskTimerGrab = metricRegistry.getTimer("dataflow-task.timer.grab") ;
    Timer dataflowTaskTimerProcess = metricRegistry.getTimer("dataflow-task.timer.process") ;
    try {
      DataflowTaskRegistry taskRegistry = dataflowRegistry.getTaskRegistry();
      VMDescriptor   vmDescriptor = workerService.getVMDescriptor();
      while(!interrupt) {
        Timer.Context dataflowTaskTimerGrabCtx = dataflowTaskTimerGrab.time() ;
        TaskContext<OperatorTaskConfig> taskContext = dataflowRegistry.getTaskRegistry().take(vmDescriptor);
        dataflowTaskTimerGrabCtx.stop();
        if(interrupt) {
          taskRegistry.suspend(vmDescriptor.getRegistryPath(), taskContext);
          doExit(TaskExecutorDescriptor.Status.TERMINATED_WITH_INTERRUPT);
          return ;
        }
        
        if(taskContext == null) {
          doExit(TaskExecutorDescriptor.Status.TERMINATED);
          return;
        }
        
        Timer.Context dataflowTaskTimerProcessCtx = dataflowTaskTimerProcess.time() ;
        taskExecutorDescriptor.addAssignedTask(taskContext.getTaskTransactionId().getTaskId());
        dataflowRegistry.
          getWorkerRegistry().
          updateWorkerTaskExecutor(vmDescriptor, taskExecutorDescriptor);
        operatorTask = new OperatorTask(workerService, taskContext);
        operatorTask.init();
        operatorTask.execute();
        if(!operatorTask.isComplete()) {
          operatorTask.suspend();
        } else {
          operatorTask.finish();
        }
        operatorTask.isIterrupted();
        dataflowTaskTimerProcessCtx.stop();
      }
      doExit(TaskExecutorDescriptor.Status.TERMINATED_WITH_INTERRUPT);
    } catch (Throwable e) {
      e.printStackTrace();
      workerService.getLogger().error("DataflowTaskExecutor Error", e);
      doExit(TaskExecutorDescriptor.Status.TERMINATED_WITH_ERROR);
    } finally {
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
}
