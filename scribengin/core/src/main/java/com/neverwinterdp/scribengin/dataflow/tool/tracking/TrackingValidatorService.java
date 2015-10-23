package com.neverwinterdp.scribengin.dataflow.tool.tracking;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;

public class TrackingValidatorService {
  private int expectNumOfMessagePerChunk = 100;
  private TrackingRegistry trackingRegistry;
  private List<TrackingMessageReader> readers = new ArrayList<>();
  private ExecutorService executorService;
  private RollableTrackingMessageReportBitSetMap bitSetReports ;
  private UpdateReportThread updateReportThread;
  
  public TrackingValidatorService(Registry registry, String reportPath) throws RegistryException {
    trackingRegistry = new TrackingRegistry(registry, reportPath, true);
  }
  
  public TrackingRegistry getTrackingRegistry() { return trackingRegistry; }
  
  public TrackingValidatorService addReader(TrackingMessageReader reader) {
    readers.add(reader);
    return this;
  }
  
  public TrackingValidatorService withExpectNumOfMessagePerChunk(int num) {
    expectNumOfMessagePerChunk = num;
    return this;
  }
  
  public void start() throws Exception {
    if(expectNumOfMessagePerChunk < 10000000) {
      bitSetReports = new RollableTrackingMessageReportBitSetMap(500);
    } else {
      bitSetReports = new RollableTrackingMessageReportBitSetMap(10);
    }
    executorService = Executors.newFixedThreadPool(readers.size());
    for(int i = 0; i < readers.size(); i++) {
      TrackingMessageReader reader = readers.get(i);
      reader.onInit(trackingRegistry);
      executorService.submit(new TrackingMessageReaderRunner(reader));
    }
    updateReportThread = new UpdateReportThread();
    updateReportThread.start();
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
    for(TrackingMessageReportBitSet sel : bitSetReports.values()) {
      trackingRegistry.updateValidatorReport(sel.updateAndGetReport());
    }
    updateReportThread.interrupt();
    for(int i = 0; i < readers.size(); i++) {
      TrackingMessageReader reader = readers.get(i);
      reader.onDestroy(trackingRegistry);
    }
  }
  
  synchronized void onTrackingMessage(TrackingMessage message) throws Exception {
    String reportName = message.reportName();
    TrackingMessageReportBitSet bitSetReport = bitSetReports.get(reportName);
    if(bitSetReport == null) {
      bitSetReport = new TrackingMessageReportBitSet(message.getVmId(), message.getChunkId(), expectNumOfMessagePerChunk);
      bitSetReports.add(reportName, bitSetReport);
      trackingRegistry.addValidatorReport(bitSetReport.getReport());
      System.err.println("Create Report " + reportName);
    }
    bitSetReport.log(message);
  }
  
  @SuppressWarnings("serial")
  public class RollableTrackingMessageReportBitSetMap extends LinkedHashMap<String, TrackingMessageReportBitSet> {
    int rollableSize ;
    
    public RollableTrackingMessageReportBitSetMap(int size) {
      this.rollableSize = size;
    }
    
    synchronized protected boolean removeEldestEntry(Map.Entry<String, TrackingMessageReportBitSet> eldest) {
      try {
        if(size() == rollableSize) {
          trackingRegistry.updateValidatorReport(eldest.getValue().updateAndGetReport());
          System.err.println("Remove BitSet Tracking: " + eldest.getKey());
          return true;
        }
      } catch (RegistryException e) {
        throw new RuntimeException(e);
      }
      return false;
    }
    
    synchronized public void update() throws Exception {
      for(TrackingMessageReportBitSet sel : values()) {
        trackingRegistry.updateValidatorReport(sel.updateAndGetReport());
      }
    }
    
    synchronized public void add(String name, TrackingMessageReportBitSet reportBitSet) throws Exception {
      put(name, reportBitSet);
    }
  }

  public class UpdateReportThread extends Thread {
    public void run() {
      try {
        while(true) {
          Thread.sleep(15000);
          bitSetReports.update();
        }
      } catch (InterruptedException e) {
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
  
  public class TrackingMessageReaderRunner implements Runnable {
    private TrackingMessageReader reader ;
    
    TrackingMessageReaderRunner(TrackingMessageReader reader) {
      this.reader = reader;
    }
    
    @Override
    public void run() {
      try {
        doRun();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    
    void doRun() throws Exception {
      TrackingMessage message = null;
      while((message = reader.next()) != null) {
        onTrackingMessage(message);
      }
    }
  }
}
