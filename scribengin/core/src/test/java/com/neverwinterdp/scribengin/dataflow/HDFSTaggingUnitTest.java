package com.neverwinterdp.scribengin.dataflow;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.scribengin.LocalScribenginCluster;
import com.neverwinterdp.scribengin.dataflow.tracking.TestHDFSTaggingLauncher;
import com.neverwinterdp.scribengin.dataflow.tracking.TrackingDataflowBuilder;
import com.neverwinterdp.scribengin.shell.ScribenginShell;

public class HDFSTaggingUnitTest  {
  final static String BASE_DIR = "build/working";
  
  LocalScribenginCluster localScribenginCluster ;
  ScribenginShell shell;
  
  @Before
  public void setup() throws Exception {
    
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
    dflBuilder.getTrackingConfig().setHDFSStorageDir(BASE_DIR + "/storage/hdfs"); 
    dflBuilder.setHDFSAggregateOutput();
    dflBuilder.setDefaultReplication(1);
    
    TestHDFSTaggingLauncher launcher = new TestHDFSTaggingLauncher();
    launcher.execute(shell, dflBuilder);
    
    dflBuilder.runMonitor(shell);
    
    launcher.onDestroy();
  }
}