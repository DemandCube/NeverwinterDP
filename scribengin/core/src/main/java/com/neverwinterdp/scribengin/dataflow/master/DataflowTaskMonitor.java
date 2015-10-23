package com.neverwinterdp.scribengin.dataflow.master;

import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.notification.Notifier;
import com.neverwinterdp.registry.task.switchable.SwitchableTaskContext;
import com.neverwinterdp.registry.task.switchable.SwitchableTaskMonitor;
import com.neverwinterdp.registry.task.switchable.SwitchableTaskRegistry;
import com.neverwinterdp.scribengin.dataflow.operator.OperatorTaskConfig;

public class DataflowTaskMonitor implements SwitchableTaskMonitor<OperatorTaskConfig> {
  private boolean finished = false;
  
  @Override
  public void onAssign(SwitchableTaskContext<OperatorTaskConfig> context) {
  }

  @Override
  public void onAvailable(SwitchableTaskContext<OperatorTaskConfig> context) {
  }

  @Override
  public void onFinish(SwitchableTaskContext<OperatorTaskConfig> context) {
    SwitchableTaskRegistry<OperatorTaskConfig> taskRegistry = context.getTaskRegistry();
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
  public void onFail(SwitchableTaskContext<OperatorTaskConfig> context) {
    SwitchableTaskRegistry<OperatorTaskConfig> taskRegistry = context.getTaskRegistry();
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
