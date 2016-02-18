package com.neverwinterdp.scribengin.dataflow.sample;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.scribengin.LocalScribenginCluster;
import com.neverwinterdp.scribengin.shell.ScribenginShell;

public class WebEventRouterLauncherUnitTest {
  LocalScribenginCluster localScribenginCluster ;
  ScribenginShell shell;
  String BASE_DIR = "build/working";
  Registry registry;
  
  /**
   * Setup a local Scribengin cluster. This sets up kafka, zk, and vm-master
   * @throws Exception
   */
  @Before
  public void setup() throws Exception{
    System.setProperty("app.home", BASE_DIR + "/scribengin");
    System.setProperty("vm.app.dir", BASE_DIR + "/scribengin");
    
    localScribenginCluster = new LocalScribenginCluster(BASE_DIR) ;
    localScribenginCluster.clean(); 
    localScribenginCluster.useLog4jConfig("classpath:scribengin/log4j/vm-log4j.properties");  
    localScribenginCluster.start();
    shell = localScribenginCluster.getShell();
  }
  
  /**
   * Destroy the local Scribengin cluster and clean up 
   * @throws Exception
   */
  @After
  public void teardown() throws Exception{
    localScribenginCluster.shutdown();
  }
  
  @Test
  public void runTest() throws Exception {
    String[] args = {
      "--local-app-home", "NA",
      "--generator-num-of-web-events", "10000",
      "--dataflow-num-of-worker", "2", "--dataflow-num-of-executor-per-worker", "4"
    };
    WebEventRouterLauncher launcher = new WebEventRouterLauncher(args);
    launcher.runWebEventGenerator();
    launcher.submitDataflow(shell);
    launcher.runMonitor(shell);
  }
}
