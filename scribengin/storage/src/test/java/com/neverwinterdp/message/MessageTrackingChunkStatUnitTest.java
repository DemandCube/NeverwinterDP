package com.neverwinterdp.message;

import org.junit.After;
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
  public void testMessageTrackingChunkStat() throws Exception {
    MessageTrackingRegistry mRegistry = new MessageTrackingRegistry(registry, "/tracking-message");
    mRegistry.initRegistry();
    
    int chunkId = mRegistry.nextMessageChunkId();
    MessageTrackingChunkStat inputChunk = new MessageTrackingChunkStat("input", chunkId, 10000);
    mRegistry.saveProgress(log(inputChunk, 0, 1000));
    
    MessageTrackingChunkStat inputChunk1 = new MessageTrackingChunkStat("input", chunkId, 10000);
    mRegistry.saveProgress(log(inputChunk1, 1000, 3000));
    registry.get("/").dump(System.out);
    
    MessageTrackingChunkStat inputChunk2 = new MessageTrackingChunkStat("input", chunkId, 10000);
    mRegistry.saveProgress(log(inputChunk2, 3000, 10000));
    registry.get("/").dump(System.out);
    
    MessageTrackingChunkStat mergeChunk = mRegistry.mergeProgress("input", chunkId);
    registry.get("/").dump(System.out);
    System.out.println(mergeChunk.toFormattedText());
    System.out.println("complete = " + mergeChunk.isComplete());
  }
  
  MessageTrackingChunkStat log(MessageTrackingChunkStat chunk, int from, int to) {
    for(int i = from; i < to; i++) {
      MessageTracking mTracking = new MessageTracking(chunk.getChunkId(), i);
      chunk.log(mTracking);
    }
    return chunk;
  }
}