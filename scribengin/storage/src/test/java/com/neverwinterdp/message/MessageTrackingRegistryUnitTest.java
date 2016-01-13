package com.neverwinterdp.message;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.RegistryException;
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
    int MAX_WINDOW_SIZE  = 10000;
    int NUM_OF_COMMITTER = 3;
    int NUM_OF_COMMIT_PER_COMMITTER = 10;
    
    TrackingWindowRegistry tRegistry = new TrackingWindowRegistry(registry, "/tracking-message");
    tRegistry.initRegistry();
    
    ExecutorService windowCommitterService = Executors.newFixedThreadPool(NUM_OF_COMMITTER);
    for(int i = 0; i < NUM_OF_COMMITTER; i++) {
      windowCommitterService.submit(new WindowCommitter(tRegistry, NUM_OF_COMMIT_PER_COMMITTER, MAX_WINDOW_SIZE));
    }
    windowCommitterService.shutdown();
    
    while(!windowCommitterService.isTerminated()) {
      Thread.sleep(3000);
      TrackingWindowReport report = tRegistry.merge();
      System.out.println(report.toDetailFormattedText());
    }
    
    TrackingWindowReport report = tRegistry.merge();
    System.out.println(report.toDetailFormattedText());
    
    registry.get("/").dump(System.out);
  }  
  
  public class WindowCommitter implements Runnable {
    TrackingWindowRegistry tRegistry;
    int numOfCommit;
    int maxWindowSize ;
    
    public WindowCommitter(TrackingWindowRegistry tRegistry, int numOfCommit, int maxWindowSize) {
      this.tRegistry = tRegistry;
      this.numOfCommit = numOfCommit;
      this.maxWindowSize = maxWindowSize;
    }
    
    @Override
    public void run() {
      try {
        doRun();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    
    public void doRun() throws RegistryException, InterruptedException {
      Random rand = new Random();
      for(int i = 0; i < numOfCommit; i++) {
        int windowId = tRegistry.nextWindowId();
        int windowSize = rand.nextInt(maxWindowSize);
        if(windowSize < 1000) windowSize = 1000;
        generateWindow(windowId, windowSize);
      }
    }
    
    public void generateWindow(int windowId, int windowSize) throws RegistryException, InterruptedException {
      TrackingWindow window = new TrackingWindow(windowId, maxWindowSize);
      if(windowSize < 1000) windowSize = 1000;
      window.setWindowSize(windowSize);
      tRegistry.saveWindow(window);
      
      TrackingWindowStat windowStat = null;
      for(int i = 0; i < windowSize; i++) {
        if(windowStat == null) {
          windowStat = new TrackingWindowStat("output", windowId, maxWindowSize);
        }
        MessageTracking mTracking = new MessageTracking(windowId, i);
        mTracking.add(new MessageTrackingLog("message-log", null));
        windowStat.log(mTracking);
        if((i + 1) % 1753 == 0) {
          windowStat.log(mTracking);
          tRegistry.saveProgress(windowStat);
          windowStat = null;
          Thread.sleep(100);
        }
      }
      if(windowStat != null) {
        tRegistry.saveProgress(windowStat);
      }
    }
  }
}