package com.neverwinterdp.registry.task.switchable;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Stage;
import com.mycila.guice.ext.closeable.CloseableInjector;
import com.mycila.guice.ext.closeable.CloseableModule;
import com.mycila.guice.ext.jsr250.Jsr250Module;
import com.neverwinterdp.module.AppServiceModule;
import com.neverwinterdp.registry.ErrorCode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.task.TaskDescriptor;
import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.util.text.TabularFormater;
import com.neverwinterdp.zookeeper.tool.server.EmbededZKServer;

public class SwitchableTaskServiceUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties") ;
  }
  
  static public String TASKS_PATH = "/tasks";
  
  static private EmbededZKServer zkServerLauncher ;

  private Injector container ;
  private Registry registry ;
  
  @BeforeClass
  static public void startServer() throws Exception {
    FileUtil.removeIfExist("./build/data", false);
    zkServerLauncher = new EmbededZKServer("./build/data/zookeeper") ;
    zkServerLauncher.start();
  }
  
  static public void stopServer() throws Exception {
    zkServerLauncher.shutdown();
  }
  
  @Before
  public void setup() throws Exception {
    registry = RegistryConfig.getDefault().newInstance();
    registry.connect();
    AppServiceModule module = new AppServiceModule(new HashMap<String, String>()) {
      @Override
      protected void configure(Map<String, String> properties) {
        bindInstance(Registry.class, registry);
      }
    };
    container = 
      Guice.createInjector(Stage.PRODUCTION, new CloseableModule(), new Jsr250Module(), module);
    registry = container.getInstance(Registry.class);
  }
  
  @After
  public void teardown() throws Exception {
    registry.rdelete(TASKS_PATH);
    registry.shutdown();
    container.getInstance(CloseableInjector.class).close();
  }

  @Test
  public void testTaskService() throws Exception {
    SwitchableTaskService<TaskDescriptor> service = new SwitchableTaskService<>(registry, TASKS_PATH, TaskDescriptor.class);
    TestTaskMonitor<TaskDescriptor> monitor = new TestTaskMonitor<TaskDescriptor>();
    service.addTaskMonitor(monitor);
    
    int NUM_OF_TASKS = 100;
    DecimalFormat seqIdFormater = new DecimalFormat("000");
    for(int i = 0; i < NUM_OF_TASKS; i++) {
      String taskId = "task-" + seqIdFormater.format(i) ;
      service.offer(taskId, new TaskDescriptor(taskId));
    }
    
    try {
      service.offer("task-000", new TaskDescriptor("task-000"));
      Assert.fail("should fail since the task-000 is already created");
    } catch(RegistryException ex) {
      Assert.assertEquals(ErrorCode.NodeExists, ex.getErrorCode());
    }
    service.getTaskRegistry().getTasksRootNode().dump(System.out);

    int NUM_OF_EXECUTORS = 20;
    ExecutorService execService = Executors.newFixedThreadPool(NUM_OF_EXECUTORS);
    for(int i = 0; i < NUM_OF_EXECUTORS; i++) {
      TaskExecutor<TaskDescriptor> executor = new TaskExecutor<>(i + "", service);
      execService.submit(executor);
    }
    execService.shutdown();
    execService.awaitTermination(30000, TimeUnit.MILLISECONDS);
    monitor.dump();
    service.getTaskRegistry().getTasksRootNode().dump(System.out);
    Assert.assertEquals(NUM_OF_TASKS, monitor.finishCounter);
    service.onDestroy();
  }
  
  static public class TaskLog {
    int availableCount, assignCount, finishCount, failCount ;
  }
  
  static public class TestTaskMonitor<T> implements SwitchableTaskMonitor<T> {
    TreeMap<String, TaskLog> taskLogs = new TreeMap<String, TaskLog>();
    int finishCounter = 0;
    
    @Override
    public void onAvailable(SwitchableTaskContext<T> context) {
      getTaskLog(context).availableCount++;
    }
    
    @Override
    public void onAssign(SwitchableTaskContext<T> context) {
      getTaskLog(context).assignCount++ ;
    }

    @Override
    public void onFinish(SwitchableTaskContext<T> context) {
      finishCounter++ ;
      getTaskLog(context).finishCount++;
    }
    
    @Override
    public void onFail(SwitchableTaskContext<T> context) {
      try {
        context.suspend("coordinator", true);
      } catch (RegistryException e) {
        e.printStackTrace();
      }
      getTaskLog(context).failCount++;
    }
    
    TaskLog getTaskLog(SwitchableTaskContext<T> context) {
      TaskLog taskLog = taskLogs.get(context.getTaskTransactionId().getTaskId()) ;
      if(taskLog == null) {
        taskLog = new TaskLog();
        taskLogs.put(context.getTaskTransactionId().getTaskId(), taskLog);
      }
      return taskLog ;
    }
    
    public void dump() {
      TabularFormater formatter = new TabularFormater("Task", "Available", "Assign", "Fail", "Finish") ;
      for(Map.Entry<String, TaskLog> entry : taskLogs.entrySet()) {
        TaskLog taskLog = entry.getValue();
        formatter.addRow(entry.getKey(), taskLog.availableCount, taskLog.assignCount, taskLog.failCount, taskLog.finishCount);
      }
      System.out.println(formatter.getFormatText());
    }
  }
  
  static public class TaskExecutor<T> implements Runnable {
    private String id;
    private SwitchableTaskService<T> taskService ;
    
    public TaskExecutor(String id, SwitchableTaskService<T> taskService ) {
      this.id = id;
      this.taskService = taskService;
    }
    
    public void run() {
      SwitchableTaskContext<T> tContext = null ;
      try {
        Random rand = new Random();
        int count = 0 ;
        while((tContext = taskService.take(id)) != null) {
          long processTime = rand.nextInt(500) + 1;
          Thread.sleep(processTime);
          int randNum = rand.nextInt(7);
          if(count > 10 || randNum  % 5 == 0) {
            taskService.finish(id, tContext.getTaskTransactionId());
          } else  if(randNum  % 3 == 0) {
            taskService.getTaskRegistry().getTasksAssignedHeartbeatNode().getChild(tContext.getTaskTransactionId().getTaskTransactionId()).delete();
          } else {
            taskService.suspend(id, tContext.getTaskTransactionId());
          }
          count++ ;
        }
      } catch(InterruptedException ex) {
      } catch(RegistryException ex) {
        ex.printStackTrace();
      }
    }
  }
}