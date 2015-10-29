package com.neverwinterdp.registry.task.dedicated;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.neverwinterdp.registry.task.TaskExecutorDescriptor;
import com.neverwinterdp.registry.task.TaskStatus;

public class TaskExecutor<T> implements Runnable {
  private DedicatedTaskService<T>       taskService;
  private TaskExecutorDescriptor        executor;
  private List<TaskSlotExecutor<T>>     taskSlotExecutors = new ArrayList<>();
  private  TaskSlotExecutor<T>          currentRunningTaskSlotExecutor;
  
  public TaskExecutor(String id, DedicatedTaskService<T> taskService) {
    executor = new TaskExecutorDescriptor(id, "NA");
    this.taskService = taskService;
  }
  
  public TaskExecutorDescriptor getTaskExecutorDescriptor () { return this.executor; }
  
  public void add(TaskSlotExecutor<T> taskSlotExecutor) {
    taskSlotExecutors.add(taskSlotExecutor);
  }
  
  public void onSwitchTaskSlot() {
    if(currentRunningTaskSlotExecutor != null) currentRunningTaskSlotExecutor.interrupt();
  }
  
  public void run() {
    try {
      while(true) {
        if(taskSlotExecutors.size() == 0) {
          taskService.idleExecutor(executor);
          Thread.sleep(5000);
        } else {
          taskService.activeExecutor(executor);
          runTaskExecutors();
        }
      }
    } catch(InterruptedException e) {
    } catch(Exception e) {
      e.printStackTrace();
    }
  }
  
  void runTaskExecutors() throws Exception {
    Iterator<TaskSlotExecutor<T>> executorItr = taskSlotExecutors.iterator();
    while(executorItr.hasNext()) {
      currentRunningTaskSlotExecutor = executorItr.next();
      currentRunningTaskSlotExecutor.clearInterrupt();

      currentRunningTaskSlotExecutor.onPreExecuteSlot();
      currentRunningTaskSlotExecutor.executeSlot();
      currentRunningTaskSlotExecutor.onPostExecuteSlot();
      
      
      DedicatedTaskContext<T> context = currentRunningTaskSlotExecutor.getTaskContext();
      if(context.isComplete()) {
        executorItr.remove();
        taskService.finish(executor, context.getTaskId(), TaskStatus.TERMINATED);
      }
    }
  }
}