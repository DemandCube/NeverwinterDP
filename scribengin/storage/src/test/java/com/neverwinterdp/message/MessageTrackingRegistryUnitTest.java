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
      executorService.submit(new TrackingMessageProducer(mRegistry, 10000));
    }
    
    executorService.shutdown();
    while(!executorService.isTerminated()) {
      executorService.awaitTermination(3, TimeUnit.SECONDS);
      registry.get("/").dump(System.out);
    }
    
    mRegistry.mergeProgress("output");
    MessageTrackingReporter outputReporter = mRegistry.mergeMessageTrackingLogChunk("output");
    System.out.println(outputReporter.toFormattedText());
    
    registry.get("/").dump(System.out);
  }
  
  static public class TrackingMessageProducer implements Runnable {
    private MessageTrackingRegistry mRegistry;
    private int                     chunkSize;
    private TrackingMessageLogger   inputLogger;
    private TrackingMessageLogger   outputLogger;
    private TrackingMessageLogger   splitterLogger;
    private TrackingMessageLogger   infoLogger;
    private TrackingMessageLogger   warnLogger;
    private TrackingMessageLogger   errorLogger;

    public TrackingMessageProducer(MessageTrackingRegistry mRegistry, int chunkSize) {
      this.mRegistry = mRegistry;
      this.chunkSize = chunkSize;
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
        doRun();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    
    private void doRun() throws Exception {
      for(int i = 0; i < 10; i++) {
        int chunkId = mRegistry.nextMessageChunkId();
        List<MessageTracking> messageTrackingChunkHolder = generateMessageTracking(chunkId);
        inputLogger.log(messageTrackingChunkHolder);
        //saveChunk("input", chunkId, messageTrackingChunkHolder);
        splitterLogger.log(messageTrackingChunkHolder);
        if(i % 3 == 0) {
          infoLogger.log(messageTrackingChunkHolder);
        } else if(i % 3 == 1) {
          warnLogger.log(messageTrackingChunkHolder);
        } else {
          errorLogger.log(messageTrackingChunkHolder);
        }
        outputLogger.log(messageTrackingChunkHolder);
        saveChunk("output", chunkId, messageTrackingChunkHolder);
      }
    }
    
    private List<MessageTracking> generateMessageTracking(int chunkId) throws Exception {
      List<MessageTracking> holder = new ArrayList<MessageTracking>();
      for(int j = 0; j < chunkSize; j++) {
        MessageTracking mTracking = new MessageTracking(chunkId, j);
        holder.add(mTracking);
      }
      return holder;
    }
    
    private void saveChunk(String name, int chunkId, List<MessageTracking> holder) throws Exception {
      int idx = 0;
      for(int i = 0; i < 3; i++) {
        MessageTrackingChunkStat chunk = new MessageTrackingChunkStat(name, chunkId, chunkSize);
        while(idx < chunkSize) {
          MessageTracking mTracking = holder.get(idx);
          chunk.log(mTracking);
          idx++;
          if(idx > (i + 1) * 3333) break;
        }
        mRegistry.saveProgress(chunk);
      }
    }
  }
  
  static public class TrackingMessageLogger  {
    private String logName;

    public TrackingMessageLogger(String name) {
      this.logName = name;
    }
    
    public void log(List<MessageTracking> mTrackings) {
      for(int i = 0; i < mTrackings.size(); i++) {
        MessageTracking mTracking = mTrackings.get(i);
        mTracking.add(new MessageTrackingLog(logName, null));
      }
    }
  }
}