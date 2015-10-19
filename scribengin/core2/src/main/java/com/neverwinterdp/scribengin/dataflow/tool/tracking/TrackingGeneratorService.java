package com.neverwinterdp.scribengin.dataflow.tool.tracking;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;

public class TrackingGeneratorService {
  private Logger logger ;

  private ChunkGenerator chunkGenerator = new ChunkGenerator();
  private long   breakInPeriod  =   1;
  
  private TrackingRegistry trackingRegistry;
  private List<TrackingMessageWriter> writers = new ArrayList<>();
  private ExecutorService executorService;
  
  public TrackingGeneratorService(Registry registry, String reportPath) throws RegistryException {
    trackingRegistry = new TrackingRegistry(registry, reportPath, true);
  }
  
  public TrackingRegistry getTrackingRegistry() { return trackingRegistry; }

  public TrackingGeneratorService withLogger(Logger logger) {
    this.logger = logger;
    return this;
  }
  
  public TrackingGeneratorService withVMId(String vmId) {
    chunkGenerator.vmId = vmId;
    return this;
  }
  
  public TrackingGeneratorService withNumOfChunk(int num) {
    chunkGenerator.numOfChunk = num;
    return this;
  }
  
  public TrackingGeneratorService withChunkSize(int size) {
    chunkGenerator.numOfMessage = size;
    return this;
  }
  
  public TrackingGeneratorService withMessageSize(int size) {
    chunkGenerator.messageSize = size;
    return this;
  }
  
  public TrackingGeneratorService withBreakInPeriod(long period) {
    this.breakInPeriod = period;
    return this;
  }
  
  public TrackingGeneratorService addWriter(TrackingMessageWriter writer) {
    writers.add(writer);
    return this;
  }
  
  public void start() throws Exception {
    executorService = Executors.newFixedThreadPool(writers.size());
    for(int i = 0; i < writers.size(); i++) {
      TrackingMessageWriter writer = writers.get(i);
      writer.onInit(trackingRegistry);
      executorService.submit(new TrackingMessageWriterRunner(writer));
    }
    executorService.shutdown();
  }
  
  public void shutdown() throws Exception {
    executorService.shutdownNow();
    onFinish();
  }
  
  public void awaitForTermination(long timeout, TimeUnit unit) throws Exception {
    executorService.awaitTermination(timeout, unit);
    onFinish();
  }
  
  void onFinish() throws Exception {
    for(int i = 0; i < writers.size(); i++) {
      TrackingMessageWriter writer = writers.get(i);
      writer.onDestroy(trackingRegistry);
    }
  }
  
  void info(String message) {
    if(logger != null) logger.info(message);
    else System.out.println("TrackingGeneratorService: " + message);
  }
  
  void error(String message, Throwable t) {
    if(logger != null) {
      logger.error(message, t);
    } else {
      System.err.println("TrackingGeneratorService: " + message);
      t.printStackTrace();
    }
  }
  
  public class TrackingMessageWriterRunner implements Runnable {
    private TrackingMessageWriter writer ;
    
    TrackingMessageWriterRunner(TrackingMessageWriter writer) {
      this.writer = writer;
    }
    
    public void run() {
      try {
        doRun();
      } catch (InterruptedException e) {
      } catch (Exception e) {
        error("Error:", e);
      }
    }
    
    public void doRun() throws Exception {
      TrackingMessage message = null;
      int count = 0 ;
      while((message = chunkGenerator.nextMessage()) != null) {
        writer.write(message);
        count++ ;
        if(breakInPeriod > 0 && count % 1000 == 0) {
          Thread.sleep(breakInPeriod);
        }
      }
    }
  }
  
  public class ChunkGenerator {
    private String vmId        = "localhost";
    private int    numOfChunk  = 5;
    private int    numOfMessage   = 1000;
    private int    messageSize = 512;

    private int    currentChunkIdTracker = 0;
    private String currentChunkId;
    private int    currentChunkMessageIdTracker = 0;
    
    private TrackingMessageReport currentReport ;
    private Random  random = new Random();
    
    synchronized public TrackingMessage nextMessage() throws Exception {
      if(currentChunkMessageIdTracker == numOfMessage) {
        
        trackingRegistry.updateGeneratorReport(currentReport);
        if(currentChunkIdTracker == numOfChunk) return null ;
        
        currentChunkId = "chunk-" + currentChunkIdTracker++; 
        currentChunkMessageIdTracker = 0;
        currentReport = new TrackingMessageReport(vmId, currentChunkId, numOfMessage);
        trackingRegistry.addGeneratorReport(currentReport);
      }
      if(currentChunkId == null) {
        currentChunkId = "chunk-" + currentChunkIdTracker++; 
        currentReport = new TrackingMessageReport(vmId, currentChunkId, numOfMessage);
        trackingRegistry.addGeneratorReport(currentReport);
      }
      
      byte[] data = randomData(messageSize);
      TrackingMessage message = new TrackingMessage(vmId, currentChunkId, currentChunkMessageIdTracker++, data);
      currentReport.setProgress(currentChunkMessageIdTracker);
      currentReport.setNoLostTo(currentChunkMessageIdTracker);
      if(currentChunkMessageIdTracker % 50000 == 0) {
        trackingRegistry.updateGeneratorReport(currentReport);
      }
      return message ;
    }
    
    byte[] randomData(int size) {
      byte[] data = new byte[size];
      random.nextBytes(data);
      return data;
    }
  }
}