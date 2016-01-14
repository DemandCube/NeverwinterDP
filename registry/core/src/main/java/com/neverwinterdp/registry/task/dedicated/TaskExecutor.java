package com.neverwinterdp.registry.task.dedicated;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.neverwinterdp.registry.task.TaskExecutorDescriptor;
import com.neverwinterdp.registry.task.TaskStatus;

final public class TaskExecutor<T> implements Runnable {
  private DedicatedTaskService<T>   taskService;
  private TaskExecutorDescriptor    executor;
  private int                       numOfTaskSlot;
  private List<TaskSlotExecutor<T>> taskSlotExecutors = new ArrayList<>();
  private TaskSlotExecutor<T>       currentRunningTaskSlotExecutor;
  private boolean                   shutdown          = false;
  private boolean                   simulateKill      = false;
  private Throwable                 error;

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
  
  public void setTickTime(long time) {
    Iterator<TaskSlotExecutor<T>> executorItr = taskSlotExecutors.iterator();
    while(executorItr.hasNext()) {
      executorItr.next().setTickTime(time);;
    }
  }
  
  public void shutdown() { shutdown = true; }
  
  public Throwable getError() { return error ; }
  
  public void simulateKill() {
    simulateKill = true;
    for(TaskSlotExecutor<T> sel : taskSlotExecutors) {
      sel.simulateKill();
    }
  }
  
  public void run() {
    try {
      while(!shutdown) {
        if(simulateKill) return;
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
      System.err.println("TaskExecutor: Catch an interrupt exception");
    } catch(Throwable e) {
      error = e ;
      e.printStackTrace();
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
  
  void runTaskExecutors() throws InterruptedException, Exception {
    Iterator<TaskSlotExecutor<T>> executorItr = taskSlotExecutors.iterator();
    long totalRuntime = 0;
    while(executorItr.hasNext()) {
      if(simulateKill) return;
      currentRunningTaskSlotExecutor = executorItr.next();
      currentRunningTaskSlotExecutor.setTickTimeout(System.currentTimeMillis() + 10000);;
      currentRunningTaskSlotExecutor.onPreExecuteSlot();
      totalRuntime += currentRunningTaskSlotExecutor.executeSlot();
      currentRunningTaskSlotExecutor.onPostExecuteSlot();
      DedicatedTaskContext<T> context = currentRunningTaskSlotExecutor.getTaskContext();
      if(context.isComplete()) {
        executorItr.remove();
        taskService.finish(executor, context.getTaskId(), TaskStatus.TERMINATED);
      }
    }
    if(totalRuntime <  1000) Thread.sleep(1000);
  }
}