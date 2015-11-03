package com.neverwinterdp.scribengin.dataflow.master;

import org.slf4j.Logger;

import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.notification.Notifier;
import com.neverwinterdp.registry.task.dedicated.DedicatedTaskMonitor;
import com.neverwinterdp.registry.task.dedicated.DedicatedTaskRegistry;
import com.neverwinterdp.scribengin.dataflow.operator.OperatorTaskConfig;

public class DataflowTaskMonitor implements DedicatedTaskMonitor<OperatorTaskConfig> {
  private boolean finished = false;
  private Logger logger ;
  
  @Override
  public void onAddExecutor(DedicatedTaskRegistry<OperatorTaskConfig> taskRegistry, String executorId) {
  }

  @Override
  public void onLostExecutor(DedicatedTaskRegistry<OperatorTaskConfig> taskRegistry, String executorId) {
    try {
      taskRegistry.historyTaskExecutor(executorId);
    } catch (RegistryException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void onAvailable(DedicatedTaskRegistry<OperatorTaskConfig> taskRegistry, String taskId) {
  }

  @Override
  public void onFinish(DedicatedTaskRegistry<OperatorTaskConfig> taskRegistry, String taskId) {
    try {
      int allTask = taskRegistry.getTasksListNode().getChildren().size();
      int finishTask = taskRegistry.getTaskFinishedNode().getChildren().size();
      if(allTask == finishTask) {
        finished = true ;
        synchronized(this) {
          notifyAll() ;
        }
      }
    } catch(Exception ex) {
      try {
        Notifier notifier = taskRegistry.getTaskCoordinationNotifier();
        notifier.error("fail-coodinate-a-finish-task", "Cannot coordinate a finished task", ex);
      } catch (RegistryException e) {
        e.printStackTrace();
      }
    }
  }

  synchronized public boolean waitForAllTaskFinish(long timeout) throws InterruptedException {
    if(finished) return finished;
    if(timeout > 0) wait(timeout) ;
    else wait() ;
    return finished;
  }

}
