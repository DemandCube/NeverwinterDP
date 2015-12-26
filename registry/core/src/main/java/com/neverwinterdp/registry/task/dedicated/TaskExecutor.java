package com.neverwinterdp.registry.task.dedicated;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.neverwinterdp.registry.task.TaskExecutorDescriptor;
import com.neverwinterdp.registry.task.TaskStatus;

final public class TaskExecutor<T> implements Runnable {
  private DedicatedTaskService<T>       taskService;
  private TaskExecutorDescriptor        executor;
  private int                           numOfTaskSlot;
  private List<TaskSlotExecutor<T>>     taskSlotExecutors = new ArrayList<>();
  private TaskSlotExecutor<T>           currentRunningTaskSlotExecutor;
  private boolean                       shutdown = false;
  private Throwable                     error ;
  
  public TaskExecutor(String id, DedicatedTaskService<T> taskService, int numOfTaskSlot) {
    executor = new TaskExecutorDescriptor(id, "NA");
    this.taskService = taskService;
    this.numOfTaskSlot = numOfTaskSlot;
  }
  
  public TaskExecutorDescriptor getTaskExecutorDescriptor () { return this.executor; }
  
  public List<TaskSlotExecutor<T>> getTaskSlotExecutors() { return taskSlotExecutors; }
  
  public void add(TaskSlotExecutor<T> taskSlotExecutor) {
    taskSlotExecutors.add(taskSlotExecutor);
  }
  
  public void broadcast(TaskExecutorEvent event) throws Exception {
    for(int i = 0; i < taskSlotExecutors.size(); i++) {
      TaskSlotExecutor<T> slotExecutor = taskSlotExecutors.get(i);
      slotExecutor.onEvent(event);
    }
  }
  
  public void onSwitchTaskSlot() {
    if(currentRunningTaskSlotExecutor != null) currentRunningTaskSlotExecutor.interrupt();
  }
  
  public void shutdown() { shutdown = true; }
  
  public Throwable getError() { return error ; }
  
  public void run() {
    try {
      while(!shutdown) {
        updateTaskSlotExecutors();
        if(taskSlotExecutors.size() == 0) {
          taskService.idleExecutor(executor);
          Thread.sleep(1000);
        } else {
          taskService.activeExecutor(executor);
          runTaskExecutors();
        }
      }
      for(TaskSlotExecutor<T> taskSlotExecutor : taskSlotExecutors) {
        taskSlotExecutor.onShutdown();
      }
    } catch(InterruptedException e) {
    } catch(Throwable e) {
      error = e ;
    }
  }
  
  void updateTaskSlotExecutors() throws Exception {
    if(taskSlotExecutors.size() < numOfTaskSlot) {
      int requestTaskSlot = numOfTaskSlot - taskSlotExecutors.size();
      List<DedicatedTaskContext<T>> contexts = taskService.getTaskRegistry().take(executor, requestTaskSlot);
      for(int j = 0; j < contexts.size(); j++) {
        TaskSlotExecutor<T> taskSlotExecutor = taskService.getTaskSlotExecutorFactory().create(contexts.get(j));
        add(taskSlotExecutor);
      }
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