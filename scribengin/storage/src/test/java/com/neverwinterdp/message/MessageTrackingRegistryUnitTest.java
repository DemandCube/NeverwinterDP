package com.neverwinterdp.message;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
    
    ExecutorService executorService = Executors.newFixedThreadPool(3);
    for(int i = 0; i < 3; i++) {
      executorService.submit(new TrackingMessageProducer(mRegistry, 10, 10000));
    }
    
    executorService.shutdown();
    while(!executorService.isTerminated()) {
      executorService.awaitTermination(3, TimeUnit.SECONDS);
      registry.get("/").dump(System.out);
    }
    
    new TrackingMessageProducer(mRegistry, 1, 10000).generateMessageTracking(mRegistry.nextMessageChunkId(), 0, 5000);
    mRegistry.mergeProgress("output");
    
    MessageTrackingReporter outputReporter = mRegistry.mergeMessageTrackingLogChunk("output");
    System.out.println(outputReporter.toFormattedText());
    
    registry.get("/").dump(System.out);
  }
  
  static public class TrackingMessageProducer implements Runnable {
    private MessageTrackingRegistry mRegistry;
    private int                     chunkSize;
    private int                     numOfChunks;
    private TrackingMessageLogger   inputLogger;
    private TrackingMessageLogger   outputLogger;
    private TrackingMessageLogger   splitterLogger;
    private TrackingMessageLogger   infoLogger;
    private TrackingMessageLogger   warnLogger;
    private TrackingMessageLogger   errorLogger;

    public TrackingMessageProducer(MessageTrackingRegistry mRegistry, int numOfChunks, int chunkSize) {
      this.mRegistry = mRegistry;
      this.chunkSize = chunkSize;
      this.numOfChunks = numOfChunks;
      inputLogger    = new TrackingMessageLogger("input");
      outputLogger   = new TrackingMessageLogger("output");
      splitterLogger = new TrackingMessageLogger("operator.splitter");
      infoLogger     = new TrackingMessageLogger("operator.persister.info");
      warnLogger     = new TrackingMessageLogger("operator.persister.warn");
      errorLogger    = new TrackingMessageLogger("operator.persister.error");
    }
    
    @Override
    public void run() {
      try {
        for(int i = 0; i < numOfChunks; i++) {
          generateMessageTracking(mRegistry.nextMessageChunkId(), 0, chunkSize);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    public void generateMessageTracking(int chunkId, int from, int to) throws Exception {
      List<MessageTracking> holder = new ArrayList<MessageTracking>();
      for(int j = from; j < to; j++) {
        MessageTracking mTracking = new MessageTracking(chunkId, j);
        inputLogger.log(mTracking);
        splitterLogger.log(mTracking);
        if(j % 3 == 0) {
          infoLogger.log(mTracking);
        } else if(j % 3 == 1) {
          warnLogger.log(mTracking);
        } else {
          errorLogger.log(mTracking);
        }
        outputLogger.log(mTracking);
        holder.add(mTracking);
      }
      saveChunk("output", chunkId, holder);
    }
    
    private void saveChunk(String name, int chunkId, List<MessageTracking> holder) throws Exception {
      MessageTrackingChunkStat chunk = new MessageTrackingChunkStat(name, chunkId, chunkSize);
      for(int i = 0; i < holder.size(); i++) {
        MessageTracking mTracking = holder.get(i);
        chunk.log(mTracking);
        if((i + 1) % 1000 == 0) {
          mRegistry.saveProgress(chunk);
          chunk = new MessageTrackingChunkStat(name, chunkId, chunkSize);
        }
      }
      if(chunk.getTrackingCount() > 0) mRegistry.saveProgress(chunk);
    }
  }
  
  static public class TrackingMessageLogger  {
    private String logName;

    public TrackingMessageLogger(String name) {
      this.logName = name;
    }
    
    public void log(MessageTracking mTracking) {
      mTracking.add(new MessageTrackingLog(logName, null));
    }
  }
}