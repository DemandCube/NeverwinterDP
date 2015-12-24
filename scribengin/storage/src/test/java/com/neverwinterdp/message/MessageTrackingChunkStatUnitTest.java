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

public class MessageTrackingChunkStatUnitTest {
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
    MessageTrackingChunkStat chunk = new MessageTrackingChunkStat("input", chunkId, 10000);
    log(chunk, 0, 1000);
    
    MessageTrackingChunkStat chunk1 = new MessageTrackingChunkStat("input", chunkId, 10000);
    log(chunk1, 1000, 3000);

    MessageTrackingChunkStat chunk2 = new MessageTrackingChunkStat("input", chunkId, 10000);
    log(chunk2, 3000, 10000);

    chunk.merge(chunk1);
    System.out.println("Merge chunk and chunk1");
    System.out.println(chunk.toFormattedText());
    
    chunk.merge(chunk2);
    System.out.println("Merge chunk, chunk1, chunk2");
    System.out.println(chunk.toFormattedText());
  }
  
  @Test
  public void testMessageTrackingChunkStat() throws Exception {
    MessageTrackingRegistry mRegistry = new MessageTrackingRegistry(registry, "/tracking-message");
    mRegistry.initRegistry();
    
    int chunkId = mRegistry.nextMessageChunkId();
    MessageTrackingChunkStat inputChunk = new MessageTrackingChunkStat("input", chunkId, 10000);
    mRegistry.saveProgress(log(inputChunk, 0, 1000));
    
    MessageTrackingChunkStat inputChunk1 = new MessageTrackingChunkStat("input", chunkId, 10000);
    mRegistry.saveProgress(log(inputChunk1, 1000, 3000));

    MessageTrackingChunkStat mergeChunk = mRegistry.mergeProgress("input", chunkId);
    Assert.assertFalse(mergeChunk.isComplete());
    mergeChunk = mRegistry.getProgress("input", chunkId);
    Assert.assertNotNull(mergeChunk);
    
    MessageTrackingChunkStat inputChunk2 = new MessageTrackingChunkStat("input", chunkId, 10000);
    mRegistry.saveProgress(log(inputChunk2, 3000, 10000));
    
    mergeChunk = mRegistry.mergeProgress("input", chunkId);
    Assert.assertTrue(mergeChunk.isComplete());
    System.out.println(mergeChunk.toFormattedText());
    
    registry.get("/").dump(System.out);
    mergeChunk = mRegistry.getFinished("input", chunkId);
    Assert.assertNotNull(mergeChunk);
  }
  
  MessageTrackingChunkStat log(MessageTrackingChunkStat chunk, int from, int to) {
    for(int i = from; i < to; i++) {
      MessageTracking mTracking = new MessageTracking(chunk.getChunkId(), i);
      mTracking.add(new MessageTrackingLog("input", null));
      chunk.log(mTracking);
    }
    return chunk;
  }
}