package com.neverwinterdp.message;

import java.util.ArrayList;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.util.log.LoggerFactory;
import com.neverwinterdp.zookeeper.tool.server.EmbededZKServerSet;

public class MessageTrackingRegistryUnitTest {
final static public String WORKING_DIR = "build/working";
  
  private EmbededZKServerSet zkCluster;
  private Registry           registry;
  
  @BeforeClass
  static public void beforeClass() throws Exception {
    LoggerFactory.log4jUseConsoleOutputConfig("WARN");
  }
  
  @Before
  public void setup() throws Exception {
    FileUtil.removeIfExist(WORKING_DIR, false);
    zkCluster = new EmbededZKServerSet(WORKING_DIR + "/zookeeper", 2181, 1);
    zkCluster.start();
    registry = RegistryConfig.getDefault().newInstance().connect();
  }
  
  @After
  public void teardown() throws Exception {
    registry.shutdown();
    zkCluster.shutdown();
  }
  
  
  @Test
  public void testMessageTrackingRegistry() throws Exception {
    MessageTrackingRegistry mRegistry = new MessageTrackingRegistry(registry, "/tracking-message");
    mRegistry.initRegistry();
    
    MessageTrackingLogChunk inputChunk1 = new MessageTrackingLogChunk("input", 1, 10000);
    mRegistry.saveMessageTrackingLogChunk(inputChunk1);
    
    MessageTrackingLogReporter inputReporter = mRegistry.mergeMessageTrackingLogChunk("input");
    
    MessageTrackingLogChunk outputChunk1 = new MessageTrackingLogChunk("output", 1, 10000);
    mRegistry.saveMessageTrackingLogChunk(outputChunk1);
    
    registry.get("/").dump(System.out);
  }
}
