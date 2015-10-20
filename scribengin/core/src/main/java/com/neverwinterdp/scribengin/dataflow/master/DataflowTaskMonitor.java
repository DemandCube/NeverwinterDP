package com.neverwinterdp.scribengin.dataflow.master;

import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.notification.Notifier;
import com.neverwinterdp.registry.task.TaskContext;
import com.neverwinterdp.registry.task.TaskMonitor;
import com.neverwinterdp.registry.task.TaskRegistry;
import com.neverwinterdp.scribengin.dataflow.operator.OperatorTaskConfig;

public class DataflowTaskMonitor implements TaskMonitor<OperatorTaskConfig> {
  private boolean finished = false;
  
  @Override
  public void onAssign(TaskContext<OperatorTaskConfig> context) {
  }

  @Override
  public void onAvailable(TaskContext<OperatorTaskConfig> context) {
  }

  @Override
  public void onFinish(TaskContext<OperatorTaskConfig> context) {
    TaskRegistry<OperatorTaskConfig> taskRegistry = context.getTaskRegistry();
    try {
      int allTask = taskRegistry.getTasksListNode().getChildren().size();
      int finishTask = taskRegistry.getTasksFinishedNode().getChildren().size();
      if(allTask == finishTask) {
        finished = true ;
        synchronized(this) {
          notifyAll() ;
        }
      }
    } catch(Exception ex) {
      Notifier notifier = taskRegistry.getTaskCoordinationNotifier();
      try {
        notifier.error("fail-coodinate-a-finish-task", "Cannot coordinate a finished task", ex);
      } catch (RegistryException e) {
        e.printStackTrace();
      }
    }
  }
  
  @Override
  public void onFail(TaskContext<OperatorTaskConfig> context) {
    TaskRegistry<OperatorTaskConfig> taskRegistry = context.getTaskRegistry();
    try {
      context.suspend("DataflowMasterService", true);
    } catch (RegistryException ex) {
      Notifier notifier = taskRegistry.getTaskCoordinationNotifier();
      try {
        notifier.error("fail-coodinate-a-fail-task", "Cannot coordinate a faild task", ex);
      } catch (RegistryException e) {
        e.printStackTrace();
      }
    }
  }

  synchronized public boolean waitForAllTaskFinish(long timeout) throws InterruptedException {
    if(timeout > 0) wait(timeout) ;
    else wait() ;
    return finished;
  }
  
}
