package com.neverwinterdp.registry;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.SequenceNumberTrackerService;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.zk.tool.server.EmbededZKServer;

public class SequenceIdTrackerUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties") ;
  }
  
  private EmbededZKServer zkServerLauncher ;
  
  @Before
  public void setup() throws Exception {
    FileUtil.removeIfExist("./build/data", false);
    
    zkServerLauncher = new EmbededZKServer("./build/data/zookeeper") ;
    zkServerLauncher.start();
  }
  
  @After
  public void teardown() throws Exception {
    zkServerLauncher.shutdown();
  }
  
  @Test
  public void testSequenceIdTracker() throws Exception {
    String SEQ_ID_TRACKER_PATH = "/id-tracker";
    SequenceIdTracker tracker1 = new  SequenceIdTracker(newRegistry().connect(), SEQ_ID_TRACKER_PATH);
    System.out.println("tracker1: " + tracker1.nextSeqId());
    SequenceIdTracker tracker2 = new  SequenceIdTracker(newRegistry().connect(), SEQ_ID_TRACKER_PATH);
    System.out.println("tracker2: " + tracker2.nextSeqId());
  }
  
  private RegistryImpl newRegistry() {
    return new RegistryImpl(RegistryConfig.getDefault()) ;
  }
}
