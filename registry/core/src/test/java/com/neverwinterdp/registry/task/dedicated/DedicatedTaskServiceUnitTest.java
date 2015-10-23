package com.neverwinterdp.registry.task.dedicated;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
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
import com.neverwinterdp.registry.task.TaskExecutorDescriptor;
import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.zookeeper.tool.server.EmbededZKServer;

public class DedicatedTaskServiceUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties") ;
  }
  
  static public String TASK_SERVICE_PATH = "/task-service";
  
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
    registry.rdelete(TASK_SERVICE_PATH);
    registry.shutdown();
    container.getInstance(CloseableInjector.class).close();
  }

  @Test
  public void testTaskService() throws Exception {
    DedicatedTaskService<TaskDescriptor> service = new DedicatedTaskService<>(registry, TASK_SERVICE_PATH, TaskDescriptor.class);
    int NUM_OF_TASKS = 15;
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
  
    
    int NUM_OF_EXECUTORS = 5;
    ExecutorService execService = Executors.newFixedThreadPool(NUM_OF_EXECUTORS);
    for(int i = 0; i < NUM_OF_EXECUTORS; i++) {
      TaskExecutor<TaskDescriptor> executor = new TaskExecutor<TaskDescriptor>("executor-" + i, service) {
        @Override
        protected TaskSlotExecutor<TaskDescriptor> createTaskSlotExecutor(TaskExecutorDescriptor executor, DedicatedTaskContext<TaskDescriptor> context) throws Exception {
          return new DummyTaskSlotExecutor(executor, context);
        }
      };
      service.addExecutor(executor.getTaskExecutorDescriptor());
      execService.submit(executor);
    }
    execService.shutdown();
    execService.awaitTermination(10000, TimeUnit.MILLISECONDS);
    service.getTaskRegistry().getTasksRootNode().dump(System.out);
    service.onDestroy();
  }
}