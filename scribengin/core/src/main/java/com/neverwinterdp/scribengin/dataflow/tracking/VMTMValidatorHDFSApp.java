package com.neverwinterdp.scribengin.dataflow.tracking;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.storage.hdfs.HDFSStorage;
import com.neverwinterdp.storage.hdfs.HDFSStorageConfig;
import com.neverwinterdp.storage.hdfs.source.HDFSSource;
import com.neverwinterdp.storage.hdfs.source.HDFSSourcePartition;
import com.neverwinterdp.storage.hdfs.source.HDFSSourcePartitionStream;
import com.neverwinterdp.storage.hdfs.source.HDFSSourcePartitionStreamReader;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.vm.VMApp;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;

public class VMTMValidatorHDFSApp extends VMApp {
  private Logger  logger;
  
  private Configuration conf;
  private HDFSStorage storage;
  
  public HDFSStorage getHDFSStorage() { return this.storage ; }
  
  @Override
  public void run() throws Exception {
    logger =  getVM().getLoggerFactory().getLogger(VMTMValidatorHDFSApp.class) ;
    VMDescriptor vmDescriptor = getVM().getDescriptor();
    VMConfig vmConfig = vmDescriptor.getVmConfig();
    
    TrackingConfig trackingConfig = vmConfig.getVMAppConfigAs(TrackingConfig.class);
    Registry registry = getVM().getVMRegistry().getRegistry();
    registry.setRetryable(true);
    
    conf = new Configuration();
    VMConfig.overrideHadoopConfiguration(getVM().getDescriptor().getVmConfig().getHadoopProperties(), conf);
    
    runValidate(registry, trackingConfig);
  }

  public void runValidate(Registry registry, TrackingConfig trackingConfig) {
    TrackingValidatorService validatorService = null;
    try {
      info("Start runValidate(...)");
      info("reportPath = "            + trackingConfig.getTrackingReportPath());
      info("maxRuntime = "            + trackingConfig.getValidatorMaxRuntime());
      HDFSStorageConfig storageConfig = new HDFSStorageConfig("aggregate", null);
      FileSystem fs = FileSystem.get(conf);
      storage = new HDFSStorage(registry,fs, storageConfig);
      
      validatorService = new TrackingValidatorService(registry, trackingConfig);
      validatorService.addReader(new HDFSTrackingMessageReader(registry));
      validatorService.start();
      validatorService.awaitForTermination(trackingConfig.getValidatorMaxRuntime(), TimeUnit.MILLISECONDS);
      info("Finish runValidate(...)");
    } catch(Throwable error) {
      error("Error:", error);
    } finally {
      try {
        validatorService.shutdown();
      } catch (Exception e) {
        error("Error:", e);
      }
    }
  }
  
  protected void runManagement(HDFSStorage storage) throws RegistryException, IOException {
    storage.doManagement();
    storage.cleanReadDataByActiveReader();
  }
  
  void info(String message) {
    if(logger != null) logger.info(message);
    else System.out.println("VMTMValidatorKafkaApp: " + message);
  }
  
  void error(String message, Throwable error) {
    if(logger != null) {
      logger.error(message, error);
    } else {
      System.out.println("VMTMValidatorKafkaApp: " + message);
      error.printStackTrace();
    }
  }
  
  
  public class HDFSTrackingMessageReader extends TrackingMessageReader {
    private BlockingQueue<TrackingMessage> tmQueue = new LinkedBlockingQueue<>(10000);
    private HDFSSourceReader hdfsSourceReader ;
    
    HDFSTrackingMessageReader(Registry registry) {
      hdfsSourceReader = new HDFSSourceReader(registry, tmQueue) ;
    }
    
    public void onInit(TrackingRegistry registry) throws Exception {
      hdfsSourceReader.start();
    }
   
    public void onDestroy(TrackingRegistry registry) throws Exception{
      hdfsSourceReader.interrupt();
    }
    
    @Override
    public TrackingMessage next() throws Exception {
      return tmQueue.poll(10, TimeUnit.MINUTES);
    }
  }
  
  public class HDFSSourceReader extends Thread {
    private Registry                       registry;
    private BlockingQueue<TrackingMessage> tmQueue;

    HDFSSourceReader(Registry registry, BlockingQueue<TrackingMessage> tmQueue) {
      this.registry = registry;
      this.tmQueue = tmQueue;
    }
    
    public void run() {
      try {
        doRun();
      } catch (Exception e) {
        error("Error:", e);
      }
    }
    
    void doRun() throws Exception {
      HDFSSource hdfsSource = storage.getSource();
      HDFSSourcePartition partition = hdfsSource.getLatestSourcePartition();

      BlockingQueue<HDFSSourcePartitionStreamReader> streamReaderQueue = new LinkedBlockingQueue<>();
      HDFSSourcePartitionStream[] stream = partition.getPartitionStreams();
      for(int i = 0; i < stream.length; i++) {
        HDFSSourcePartitionStreamReader reader = stream[i].getReader("validator");
        streamReaderQueue.put(reader);
      }
      
      ExecutorService validatorService = Executors.newFixedThreadPool(3);
      for(int i = 0; i < 5; i++) {
        validatorService.submit(new HDFSPartitionStreamReader(streamReaderQueue, tmQueue));
      }
      
      validatorService.shutdown();
      while(!validatorService.awaitTermination(3, TimeUnit.MINUTES)) {
        runManagement(storage);
      }
    }
  }
  
  class HDFSPartitionStreamReader implements Runnable {
    private BlockingQueue<HDFSSourcePartitionStreamReader> streamReaderQueue;
    private BlockingQueue<TrackingMessage>                 tmQueue;
    
    HDFSPartitionStreamReader(BlockingQueue<HDFSSourcePartitionStreamReader> queue, BlockingQueue<TrackingMessage> tmQueue) {
      this.streamReaderQueue = queue ;
      this.tmQueue           = tmQueue;
    }
    
    @Override
    public void run() {
      try {
        doRun();
      } catch (Exception e) {
        error("Error:", e);
      }
    }
    
    void doRun() throws Exception {
      while(true) {
        HDFSSourcePartitionStreamReader streamReader = streamReaderQueue.poll(10, TimeUnit.MILLISECONDS);
        Message message = null;
        int count = 0;
        while((message = nextWithRetry(streamReader, 100, 3)) != null) {
          byte[] data = message.getData();
          TrackingMessage tMesg = JSONSerializer.INSTANCE.fromBytes(data, TrackingMessage.class);
          if(!tmQueue.offer(tMesg, 90000, TimeUnit.MILLISECONDS)) {
            throw new Exception("Cannot queue the messages after 90s, increase the buffer");
          }
          count++;
          //if(count > 10000) break;
        }
        streamReader.commit();
        streamReaderQueue.put(streamReader);
      }
    }
    
    Message nextWithRetry(HDFSSourcePartitionStreamReader streamReader, long maxWait, int numOfRetry) throws Exception {
      Exception error = null;
      for(int i = 0; i < numOfRetry; i++) {
        try {
          return streamReader.next(maxWait);
        } catch(Exception ex) {
          error = ex;
          info("Error when try to read from the HDFSSourcePartitionStreamReader. Rollback and retry. try = " + i + ", " + ex.getMessage());
          streamReader.rollback();
        }
      }
      throw error;
    }
  }
}