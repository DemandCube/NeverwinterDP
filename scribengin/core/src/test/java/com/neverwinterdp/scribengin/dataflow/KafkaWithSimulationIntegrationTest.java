package com.neverwinterdp.scribengin.dataflow;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.registry.txevent.TXEvent;
import com.neverwinterdp.scribengin.LocalScribenginCluster;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.tracking.TrackingDataflowBuilder;
import com.neverwinterdp.scribengin.dataflow.tracking.TrackingMessage;
import com.neverwinterdp.scribengin.dataflow.tracking.TrackingWithSimulationLauncher;
import com.neverwinterdp.scribengin.shell.ScribenginShell;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.vm.client.VMClient;

public class KafkaWithSimulationIntegrationTest  {
  LocalScribenginCluster localScribenginCluster ;
  ScribenginShell shell;
  
  @Before
  public void setup() throws Exception {
    String BASE_DIR = "build/working";
    System.setProperty("app.home", BASE_DIR + "/scribengin");
    System.setProperty("vm.app.dir", BASE_DIR + "/scribengin");
    
    localScribenginCluster = new LocalScribenginCluster(BASE_DIR) ;
    localScribenginCluster.clean(); 
    localScribenginCluster.useLog4jConfig("classpath:scribengin/log4j/vm-log4j.properties");  
    localScribenginCluster.start();
    
    shell = localScribenginCluster.getShell();
  }
  
  @After
  public void teardown() throws Exception {
    localScribenginCluster.shutdown();
  }
  
  @Test
  public void testTracking() throws Exception {
    TrackingDataflowBuilder dflBuilder = new TrackingDataflowBuilder("tracking");
    dflBuilder.getTrackingConfig().setNumOfMessagePerChunk(30000);
    dflBuilder.getTrackingConfig().setKafkaMessageWaitTimeout(90000);
    dflBuilder.setMaxRuntime(300000);
    dflBuilder.setSlidingWindowSize(300);
    
    StartStopThread startStopThread = new StartStopThread(dflBuilder);
    startStopThread.start();
    
    dflBuilder.runMonitor(shell);
    shell.execute("registry dump");
  }
  
  public class StartStopThread extends Thread {
    TrackingDataflowBuilder dflBuilder;
    
    int startCount = 0;
    int stopCount  = 0;
    
    public StartStopThread(TrackingDataflowBuilder dflBuilder) {
      this.dflBuilder = dflBuilder;
    }
    
    public void run() {
      try {
        TrackingWithSimulationLauncher launcher = new TrackingWithSimulationLauncher();
        launcher.execute(shell, dflBuilder);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}