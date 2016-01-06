package com.neverwinterdp.message;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.util.log.LoggerFactory;
import com.neverwinterdp.zookeeper.tool.server.EmbededZKServerSet;

public class WindowMessageTrackingStatUnitTest {
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
  public void testMerge() throws Exception {
    int chunkId = 1;
    WindowMessageTrackingStat chunk = new WindowMessageTrackingStat("input", chunkId, 10000);
    log(chunk, 0, 1000);
    
    WindowMessageTrackingStat chunk1 = new WindowMessageTrackingStat("input", chunkId, 10000);
    log(chunk1, 1000, 3000);

    WindowMessageTrackingStat chunk2 = new WindowMessageTrackingStat("input", chunkId, 10000);
    log(chunk2, 3000, 10000);

    chunk.merge(chunk1);
    System.out.println("Merge chunk and chunk1");
    System.out.println(chunk.toFormattedText());
    
    chunk.merge(chunk2);
    System.out.println("Merge chunk, chunk1, chunk2");
    System.out.println(chunk.toFormattedText());
  }
  
  @Test
  public void testWindowMessageTrackingStat() throws Exception {
    MessageTrackingRegistry mRegistry = new MessageTrackingRegistry(registry, "/tracking-message");
    mRegistry.initRegistry();
    
    int windowId = mRegistry.nextWindowId("input", 3);
    WindowMessageTrackingStat inputWindow1 = new WindowMessageTrackingStat("input", windowId, 10000);
    mRegistry.saveProgress(log(inputWindow1, 0, 1000));
    
    WindowMessageTrackingStat inputWindow2 = new WindowMessageTrackingStat("input", windowId, 10000);
    mRegistry.saveProgress(log(inputWindow2, 1000, 3000));

    WindowMessageTrackingStat mergeWindow = mRegistry.mergeProgress("input", windowId);
    Assert.assertFalse(mergeWindow.isComplete());
    mergeWindow = mRegistry.getProgress("input", windowId);
    Assert.assertNotNull(mergeWindow);
    
    WindowMessageTrackingStat inputWindow3 = new WindowMessageTrackingStat("input", windowId, 10000);
    mRegistry.saveProgress(log(inputWindow3, 3000, 10000));
    
    mergeWindow = mRegistry.mergeProgress("input", windowId);
    Assert.assertTrue(mergeWindow.isComplete());
    System.out.println(mergeWindow.toFormattedText());
    
    registry.get("/").dump(System.out);
    mergeWindow = mRegistry.getFinished("input", windowId);
    Assert.assertNotNull(mergeWindow);
  }
  
  WindowMessageTrackingStat log(WindowMessageTrackingStat chunk, int from, int to) {
    for(int i = from; i < to; i++) {
      MessageTracking mTracking = new MessageTracking(chunk.getWindowId(), i);
      mTracking.add(new MessageTrackingLog("input", null));
      chunk.log(mTracking);
    }
    return chunk;
  }
}