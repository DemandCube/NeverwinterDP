package com.neverwinterdp.scribengin.dataflow;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.scribengin.LocalScribenginCluster;
import com.neverwinterdp.scribengin.dataflow.tracking.TrackingDataflowBuilder;
import com.neverwinterdp.scribengin.dataflow.tracking.TrackingMessage;
import com.neverwinterdp.scribengin.shell.ScribenginShell;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.client.VMSubmitter;

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
    VMClient vmClient = shell.getVMClient();
    String dfsAppHome = "";
    
    TrackingDataflowBuilder dflBuilder = new TrackingDataflowBuilder("tracking");
    dflBuilder.getTrackingConfig().setHDFSStorageDir(BASE_DIR + "/storage/hdfs"); 
    dflBuilder.setHDFSAggregateOutput();
    
    VMConfig vmGeneratorConfig = dflBuilder.buildVMTMGeneratorKafka();
    new VMSubmitter(vmClient, dfsAppHome, vmGeneratorConfig).submit().waitForRunning(30000);
    
    Dataflow<TrackingMessage, TrackingMessage> dfl = dflBuilder.buildDataflow();
    dfl.setDefaultReplication(1);
    DataflowDescriptor dflDescriptor = dfl.buildDataflowDescriptor();
    System.out.println(JSONSerializer.INSTANCE.toString(dflDescriptor));
    
    new DataflowSubmitter(shell.getScribenginClient(), dfl).submit().waitForDataflowRunning(60000);
    
    VMConfig vmValidatorConfig = dflBuilder.buildHDFSVMTMValidator();
    new VMSubmitter(vmClient, dfsAppHome, vmValidatorConfig).submit().waitForRunning(30000);
    
    dflBuilder.runMonitor(shell);
  }
}