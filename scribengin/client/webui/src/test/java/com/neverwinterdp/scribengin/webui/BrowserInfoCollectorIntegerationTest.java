package com.neverwinterdp.scribengin.webui;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.analytics.web.gripper.GripperServer;
import com.neverwinterdp.scribengin.LocalScribenginCluster;
import com.neverwinterdp.scribengin.shell.ScribenginShell;
/**
 * @author Tuan Nguyen
 * @email  tuan08@gmail.com
 */
public class BrowserInfoCollectorIntegerationTest {
  WebuiServer            httpServer;
  GripperServer          gripperServer;
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
    
    String[] webuiServerConfig = {
      "--www-dir", "src/main/webapp", 
      "--zk-connects", localScribenginCluster.getKafkaCluster().getZKConnect()
    };
    
    httpServer = new WebuiServer(webuiServerConfig);
    httpServer.start();
    
    gripperServer = new GripperServer();
    gripperServer.start();
  }
  
  @After
  public void teardown() throws Exception {
    httpServer.shutdown();
    localScribenginCluster.shutdown();
  }
  
  @Test
  public void test() throws Exception {
    Thread.sleep(100000000);
  }
}