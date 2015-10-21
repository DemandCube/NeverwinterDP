package com.neverwinterdp.registry.activity;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
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
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.zookeeper.tool.server.EmbededZKServer;

public class ActivityServiceUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties") ;
  }
  
  static public String ACTIVITIES_PATH = HelloActivityCoordinator.ACTIVITIES_PATH;
  static private EmbededZKServer zkServerLauncher ;

  private Injector container ;
  private Registry registry ;
  
  @BeforeClass
  static public void startServer() throws Exception {
    FileUtil.removeIfExist("./build/data", false);
    zkServerLauncher = new EmbededZKServer("./build/data/zookeeper") ;
    zkServerLauncher.start();
  }
  
  @AfterClass
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
    registry.rdelete(ACTIVITIES_PATH);
    registry.shutdown();
    container.getInstance(CloseableInjector.class).close();
  }

  @Test
  public void testActivityService() throws Exception {
    ActivityService service = new ActivityService(container, ACTIVITIES_PATH) ;
    
    Activity activityCreate = service.create(new HelloActivityBuilder().build());
    
    Activity activityGet = service.getActivity(activityCreate.getId()) ;
    Assert.assertEquals(activityCreate.getId(), activityGet.getId());
    
    List<ActivityStep> activityStepsGet = service.getActivitySteps(activityCreate);
    Assert.assertEquals(10, activityStepsGet.size());
    
    ActivityStep activityStep0 = activityStepsGet.get(0) ;
    service.updateActivityStepExecuting(activityCreate, activityStep0, new HelloActivityStepWorkerDescriptor());
    Assert.assertEquals(
        ActivityStep.Status.EXECUTING,
        service.getActivityStep(activityCreate.getId(), activityStep0.getId()).getStatus());

    service.updateActivityStepFinished(activityCreate, activityStep0);
    Assert.assertEquals(
        ActivityStep.Status.FINISHED,
        service.getActivityStep(activityCreate.getId(), activityStep0.getId()).getStatus());
    
    Assert.assertEquals(1, service.getActivities().size());
    registry.get("/").dump(System.out);
    
    service.history(activityGet);
    Assert.assertEquals(1, service.getHistoryActivities().size());
    registry.get("/").dump(System.out);
    service.onDestroy();
    System.err.println("Done!!!!!!!!!!!");
  }
  
  @Test
  public void testRunActivity() throws Exception {
    ActivityService service = new ActivityService(container, ACTIVITIES_PATH) ;
    Activity activity = new HelloActivityBuilder().build() ;
    
    ActivityExecutionContext context1 = service.run(activity);
    context1.waitForTermination(5000);
    
    ActivityCoordinator helloCoordinator = service.getActivityCoordinator(activity.getCoordinator()) ;
    Assert.assertEquals(context1.getActivityCoordinator(), helloCoordinator) ;
    registry.get("/").dump(System.out);
    service.onDestroy();
  }
  
  @Test
  public void testQueueActivity() throws Exception {
    ActivityService service = new ActivityService(container, ACTIVITIES_PATH) ;
    Activity activity = new HelloActivityBuilder().build() ;
    
    service.queue(activity);
    Thread.sleep(3000);
    registry.get("/").dump(System.out);
    service.onDestroy();
  }
  
  @Test
  public void testResumeActivity() throws Exception {
    ActivityService service1 = new ActivityService(container, ACTIVITIES_PATH) ;
    Activity activity = new HelloActivityBuilder().build() ;
    service1.queue(activity);
    Thread.sleep(250);
    registry.shutdown();
    Thread.sleep(3000);
    registry.connect();
    service1.onDestroy();
    
    registry.get("/").dump(System.out);
    ActivityService service2 = new ActivityService(container, ACTIVITIES_PATH) ;
    Thread.sleep(5000);
    registry.get("/").dump(System.out);
    service2.onDestroy();
  }
  
  static public class HelloActivityStepWorkerDescriptor {
    String refPath = "some/path";

    public String getRefPath() { return refPath; }

    public void setRefPath(String refPath) { this.refPath = refPath; }
  }
}