package com.neverwinterdp.scribengin.dataflow;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.config.DataflowConfig;
import com.neverwinterdp.scribengin.tool.EmbededVMClusterBuilder;
import com.neverwinterdp.scribengin.tool.ScribenginClusterBuilder;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.util.io.IOUtil;

public class DataflowUnitTest {
  static {
    System.setProperty("java.net.preferIPv4Stack", "true") ;
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties") ;
  }
  
  protected ScribenginClusterBuilder clusterBuilder ;
  
  @Before
  public void setup() throws Exception {
    clusterBuilder = new ScribenginClusterBuilder(new EmbededVMClusterBuilder()) ;
    clusterBuilder.clean(); 
    clusterBuilder.startVMMasters();
  }
  
  @After
  public void teardown() throws Exception {
    clusterBuilder.shutdown();
  }
  
  @Test
  public void testDataflow() throws Exception {
    System.out.println("Test Dataflow");
    ScribenginClient scribenginClient = clusterBuilder.getScribenginClient();
    Registry registry = scribenginClient.getRegistry();
    
    String json = IOUtil.getFileContentAsString("src/test/resources/dataflow-config.json");
    DataflowConfig config = JSONSerializer.INSTANCE.fromString(json, DataflowConfig.class);
    DataflowSubmitter submitter = new DataflowSubmitter(scribenginClient);
    submitter.submit(config);
    Thread.sleep(5000);
    registry.get("/").dump(System.out);
  }
}