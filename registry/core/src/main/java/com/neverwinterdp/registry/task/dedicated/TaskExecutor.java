package com.neverwinterdp.registry.task.dedicated;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.neverwinterdp.registry.task.TaskExecutorDescriptor;
import com.neverwinterdp.registry.task.TaskStatus;

abstract public class TaskExecutor<T> implements Runnable {
  private DedicatedTaskService<T>       taskService;
  private TaskExecutorDescriptor        executor;
  private List<TaskSlotExecutor<T>>     taskExecutors = new ArrayList<>();
  private int                           maxNumOfTasks = 3;
  
  public TaskExecutor(String id, DedicatedTaskService<T> taskService) {
    executor = new TaskExecutorDescriptor(id, "NA");
    this.taskService = taskService;
  }
  
  public TaskExecutorDescriptor getTaskExecutorDescriptor () { return this.executor; }
  
  public void run() {
    try {
      while(true) {
        updateTaskExecutors();
        if(taskExecutors.size() == 0) {
          taskService.idleExecutor(executor);
          Thread.sleep(5000);
        } else {
          taskService.idleExecutor(executor);
          runTaskExecutors();
        }
      }
    } catch(Exception e) {
      e.printStackTrace();
    }
  }
  
  
  void updateTaskExecutors() throws Exception {
    int numOfTask = maxNumOfTasks - taskExecutors.size() ;
    if(numOfTask > 0) {
      List<DedicatedTaskContext<T>> contexts = taskService.getTaskRegistry().take(executor, maxNumOfTasks);
      for(int i = 0; i < contexts.size(); i++) {
        taskExecutors.add(createTaskSlotExecutor(executor, contexts.get(i)));
      }
    }
  }
  
  void runTaskExecutors() throws Exception {
    Iterator<TaskSlotExecutor<T>> executorItr = taskExecutors.iterator();
    while(executorItr.hasNext()) {
      TaskSlotExecutor<T> taskExecutor = executorItr.next();
      taskExecutor.onPreExecuteSlot();
      taskExecutor.executeSlot();
      taskExecutor.onPostExecuteSlot();
      
      DedicatedTaskContext<T> context = taskExecutor.getTaskContext();
      if(context.isComplete()) {
        executorItr.remove();
        taskService.finish(executor, context.getTaskId(), TaskStatus.TERMINATED);
      }
    }
  }
  
  abstract protected TaskSlotExecutor<T> createTaskSlotExecutor(TaskExecutorDescriptor executor, DedicatedTaskContext<T> context) throws Exception ;
  
}