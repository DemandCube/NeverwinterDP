package com.neverwinterdp.message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
    String[] logName = {"input", "splitter", "output"};
    for(int i = 0; i < 3; i++) {
      executorService.submit(new TrackingMessageProducer(mRegistry, logName, 10000));
    }
    
    executorService.shutdown();
    while(!executorService.isTerminated()) {
      executorService.awaitTermination(3, TimeUnit.SECONDS);
      for(String selLogName : logName) {
        MessageTrackingReporter reporter = mRegistry.mergeMessageTrackingLogChunk(selLogName);
        registry.get("/").dump(System.out);
        System.out.println(reporter.toFormattedText());
      }
    }
  }
  
  static public class TrackingMessageProducer implements Runnable {
    private MessageTrackingRegistry          mRegistry;
    private String[]                         logName;
    private int                              chunkSize;
    private List<TrackingMessageLogProducer> logProducers = new ArrayList<>();

    public TrackingMessageProducer(MessageTrackingRegistry mRegistry, String[] logName, int chunkSize) {
      this.mRegistry = mRegistry;
      this.logName      = logName ;
      this.chunkSize = chunkSize;
     for(int i = 0; i < logName.length; i++) {
       TrackingMessageLogProducer logProducer = new TrackingMessageLogProducer(mRegistry, logName[i], chunkSize);
       logProducers.add(logProducer);
     }
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
      for(int i = 0; i < 10; i++) generateChunk();
    }
    
    private void generateChunk() throws Exception {
      int chunkId = mRegistry.nextMessageChunkId();
      for(int j = 0; j < chunkSize; j++) {
        MessageTracking mTracking = new MessageTracking(chunkId, j);
        for(int k = 0; k < logProducers.size(); k++) {
          TrackingMessageLogProducer logProducer = logProducers.get(k);
          logProducer.produce(mTracking);
        }
        if(j % 3000 == 0) flush();
      }
      flush();
    }
    
    private void flush() throws Exception {
      for(int k = 0; k < logProducers.size(); k++) {
        TrackingMessageLogProducer logProducer = logProducers.get(k);
        logProducer.flush();
      }
    }
  }
  
  static public class TrackingMessageLogProducer  {
    private MessageTrackingRegistry               mRegistry;
    private String                                logName;
    private int                                   chunkSize;
    private Map<Integer, MessageTrackingChunkStat> chunks = new HashMap<>();
    
    public TrackingMessageLogProducer(MessageTrackingRegistry mRegistry, String name, int chunkSize) {
      this.mRegistry    = mRegistry;
      this.logName      = name;
      this.chunkSize    = chunkSize;
    }
    
    public void produce(MessageTracking mTracking) {
      mTracking.add(new MessageTrackingLog(logName, null));
      MessageTrackingChunkStat chunk = chunks.get(mTracking.getChunkId());
      if(chunk == null) {
        chunk = new MessageTrackingChunkStat(logName, mTracking.getChunkId(), chunkSize);
        chunks.put(chunk.getChunkId(), chunk);
      }
      chunk.log(mTracking);
    }
    
    public void flush() throws Exception {
      Iterator<MessageTrackingChunkStat> i = chunks.values().iterator();
      while(i.hasNext()) {
        MessageTrackingChunkStat chunk = i.next();
        chunk.update();
        mRegistry.saveMessageTrackingChunkStat(chunk);
        if(chunk.isComplete()) i.remove();
        Thread.sleep(250);
      }
    }
  }
}