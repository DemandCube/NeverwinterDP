package com.neverwinterdp.scribengin.dataflow.tool.tracking;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.zk.tool.server.EmbededZKServer;

public class TrackingUnitTest {
  static public String TRACKING_REPORT_PATH  = "/tracking" ;
  
  static private EmbededZKServer zkServerLauncher ;
  
  private Registry registry ;
  
  @BeforeClass
  static public void startServer() throws Exception {
    FileUtil.removeIfExist("./build/data", false);
    zkServerLauncher = new EmbededZKServer("./build/data/zookeeper") ;
    zkServerLauncher.start();
  }
  
  @AfterClass
  static public void stopServer() throws Exception {
    zkServerLauncher.shutdown();
  }
  
  @Before
  public void setup() throws Exception {
    registry = RegistryConfig.getDefault().newInstance();
    registry.connect();
    registry.createIfNotExist(TRACKING_REPORT_PATH);
  }
  
  @After
  public void teardown() throws Exception {
    registry.rdelete(TRACKING_REPORT_PATH);
    registry.shutdown();
  }
  
  @Test
  public void testTracking() throws Exception {
    int NUM_OF_MESSAGE_PER_CHUNK = 1000;
    MemTrackingMessageStorage storage = new MemTrackingMessageStorage();
    
    TrackingGeneratorService generatorService = 
        new TrackingGeneratorService(registry, TRACKING_REPORT_PATH).
        withNumOfChunk(3).withChunkSize(NUM_OF_MESSAGE_PER_CHUNK).withBreakInPeriod(1);
    generatorService.
      addWriter(storage.newWriter()).
      addWriter(storage.newWriter());
    
    generatorService.start();
   
    TrackingValidatorService validatorService = new TrackingValidatorService(registry, TRACKING_REPORT_PATH);
    validatorService.
      withExpectNumOfMessagePerChunk(NUM_OF_MESSAGE_PER_CHUNK).
      addReader(storage.newReader()).
      addReader(storage.newReader());
    validatorService.start();
    validatorService.awaitForTermination(1, TimeUnit.MINUTES);
    
    List<TrackingMessageReport> generatorReports = generatorService.getTrackingRegistry().getGeneratorReports();
    System.out.println(TrackingMessageReport.getFormattedReport("Generator", generatorReports));
    
    List<TrackingMessageReport> validatorReports = validatorService.getTrackingRegistry().getValidatorReports();
    System.out.println(TrackingMessageReport.getFormattedReport("Validator", validatorReports));
   
  }
}