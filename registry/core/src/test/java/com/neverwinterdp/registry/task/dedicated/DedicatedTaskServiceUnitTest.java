package com.neverwinterdp.registry.task.dedicated;

import java.util.HashMap;
import java.util.Map;

import org.junit.After;
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
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.task.TaskDescriptor;
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
    registry.get("/").dump(System.out);
  }
}