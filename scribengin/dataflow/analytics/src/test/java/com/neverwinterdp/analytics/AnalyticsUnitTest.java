package com.neverwinterdp.analytics;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.analytics.dataflow.AanalyticsDataflowBuilder;
import com.neverwinterdp.analytics.odyssey.generator.OdysseyEventGeneratorServer;
import com.neverwinterdp.analytics.web.gripper.GripperServer;
import com.neverwinterdp.analytics.web.gripper.generator.EventGeneratorServer;
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
    int NUM_OF_ODYSSEY_EVENTS = 1000;
    int NUM_OF_WEB_EVENTS     = 3000;
    int ALL_EVENTS = NUM_OF_ODYSSEY_EVENTS + NUM_OF_WEB_EVENTS ;
    
    String[] odysseyGeneratorConfig = {
      "--num-of-workers", "1", "--zk-connects", "127.0.0.1:2181", 
      "--topic", "odyssey.input", "--num-of-events", Integer.toString(NUM_OF_ODYSSEY_EVENTS)
    };
    OdysseyEventGeneratorServer odysseyEventGeneratorServer = new OdysseyEventGeneratorServer(odysseyGeneratorConfig); 
    odysseyEventGeneratorServer.start();
    
    String[] webEventGeneratorConfig = {
      "--num-of-pages", Integer.toString(NUM_OF_WEB_EVENTS),
      "--num-of-threads", "3", "--max-visit-time", "50", "--min-visit-time", "0"
    };
    EventGeneratorServer wGeneratorServer = new EventGeneratorServer(webEventGeneratorConfig);
    wGeneratorServer.start();
    
    AanalyticsDataflowBuilder dflBuilder = new AanalyticsDataflowBuilder() ;
    Dataflow dfl = dflBuilder.buildDataflow();
    
    try {
      new DataflowSubmitter(shell.getScribenginClient(), dfl).submit().waitForDataflowRunning(60000);
    } catch (Exception ex) {
      shell.execute("registry dump");
      throw ex;
    }
    
    dflBuilder.runMonitor(shell, ALL_EVENTS, true);
  }
}