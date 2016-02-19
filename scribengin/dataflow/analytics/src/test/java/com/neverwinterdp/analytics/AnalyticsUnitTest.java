package com.neverwinterdp.analytics;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.analytics.dataflow.WADataflowBuilder;
import com.neverwinterdp.analytics.web.WebEvent;
import com.neverwinterdp.analytics.web.generator.GeneratorServer;
import com.neverwinterdp.analytics.web.gripper.GripperServer;
import com.neverwinterdp.scribengin.LocalScribenginCluster;
import com.neverwinterdp.scribengin.dataflow.Dataflow;
import com.neverwinterdp.scribengin.dataflow.DataflowSubmitter;
import com.neverwinterdp.scribengin.shell.ScribenginShell;
/**
 * @author Tuan Nguyen
 * @email  tuan08@gmail.com
 */
public class AnalyticsUnitTest {
  GripperServer          server;
  LocalScribenginCluster localScribenginCluster;
  ScribenginShell        shell;

  @Before
  public void setup() throws Exception {
    String BASE_DIR = "build/working";
    System.setProperty("app.home",   BASE_DIR + "/scribengin");
    System.setProperty("vm.app.dir", BASE_DIR + "/scribengin");
    
    localScribenginCluster = new LocalScribenginCluster(BASE_DIR) ;
    localScribenginCluster.clean(); 
    localScribenginCluster.useLog4jConfig("classpath:scribengin/log4j/vm-log4j.properties");  
    localScribenginCluster.start();
    
    shell = localScribenginCluster.getShell();
    
    server = new GripperServer();
    server.start();
  }
  
  @After
  public void teardown() throws Exception {
    server.shutdown();
    localScribenginCluster.shutdown();
  }
  
  @Test
  public void test() throws Exception {
    int NUM_OF_VISIT_PAGES = 100000;
    GeneratorServer generatorServer = new GeneratorServer();
    generatorServer.setNumOfVisitPages(NUM_OF_VISIT_PAGES);
    generatorServer.start();
    
    WADataflowBuilder dflBuilder = new WADataflowBuilder() ;
    Dataflow<WebEvent, WebEvent> dfl = dflBuilder.buildDataflow();
    
    try {
      new DataflowSubmitter(shell.getScribenginClient(), dfl).submit().waitForDataflowRunning(60000);
    } catch (Exception ex) {
      shell.execute("registry dump");
      throw ex;
    }
    
    dflBuilder.runMonitor(shell, NUM_OF_VISIT_PAGES);
  }
}