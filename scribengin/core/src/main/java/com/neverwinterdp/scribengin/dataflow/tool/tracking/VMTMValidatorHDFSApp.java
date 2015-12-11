package com.neverwinterdp.scribengin.dataflow.tool.tracking;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.storage.Record;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.hdfs.HDFSStorage;
import com.neverwinterdp.storage.hdfs.source.HDFSSource;
import com.neverwinterdp.storage.hdfs.source.HDFSSourcePartition;
import com.neverwinterdp.storage.hdfs.source.HDFSSourcePartitionStream;
import com.neverwinterdp.storage.hdfs.source.HDFSSourcePartitionStreamReader;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.vm.VMApp;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;

public class VMTMValidatorHDFSApp extends VMApp {
  private Logger logger;
  
  @Override
  public void run() throws Exception {
    logger =  getVM().getLoggerFactory().getLogger(VMTMValidatorHDFSApp.class) ;
    logger.info("Start run()");
    VMDescriptor vmDescriptor = getVM().getDescriptor();
    VMConfig vmConfig = vmDescriptor.getVmConfig();
    Registry registry = getVM().getVMRegistry().getRegistry();
    registry.setRetryable(true);
    
    String reportPath   = vmConfig.getProperty("tracking.report-path", "/applications/tracking-message");
    int    numOfReader  = vmConfig.getPropertyAsInt("tracking.num-of-reader", 3);
    long   maxRuntime   = vmConfig.getPropertyAsLong("tracking.max-runtime", 120000);
    int    expectNumOfMessagePerChunk = vmConfig.getPropertyAsInt("tracking.expect-num-of-message-per-chunk", 0);
    
    String storageRegPath      = vmConfig.getProperty("storage.registry.path", "/storage/hdfs/tracking-aggregate");
    long   partitionRollPeriod = vmConfig.getPropertyAsLong("hdfs.partition-roll-period", (15 * 60 * 1000));
    
    logger.info("reportPath = "            + reportPath);
    logger.info("numOfReader = "           + numOfReader);
    logger.info("maxRuntime = "            + maxRuntime);
    logger.info("storage registry path = " + storageRegPath);
    logger.info("partitionRollPeriod = "   + partitionRollPeriod);
    
    TrackingValidatorService validatorService = new TrackingValidatorService(registry, reportPath);
    validatorService.withExpectNumOfMessagePerChunk(expectNumOfMessagePerChunk);
    validatorService.addReader(
        new HDFSTrackingMessageReader(registry, storageRegPath)
    );
    validatorService.start();
    validatorService.awaitForTermination(maxRuntime, TimeUnit.MILLISECONDS);
  }

  public class HDFSTrackingMessageReader extends TrackingMessageReader {
    private BlockingQueue<TrackingMessage> tmQueue = new LinkedBlockingQueue<>(10000);
    private HDFSSourceReader hdfsSourceReader ;
    
    HDFSTrackingMessageReader(Registry registry, String storageRegPath) {
      hdfsSourceReader = new HDFSSourceReader(registry, storageRegPath, tmQueue) ;
    }
    
    public void onInit(TrackingRegistry registry) throws Exception {
      hdfsSourceReader.start();
    }
   
    public void onDestroy(TrackingRegistry registry) throws Exception{
      hdfsSourceReader.interrupt();
    }
    
    @Override
    public TrackingMessage next() throws Exception {
      return tmQueue.poll(5, TimeUnit.MINUTES);
    }
  }
  
  public class HDFSSourceReader extends Thread {
    private Registry                       registry;
    private StorageConfig                  storageConfig;
    private BlockingQueue<TrackingMessage> tmQueue;

    HDFSSourceReader(Registry registry, String registryPath, BlockingQueue<TrackingMessage> tmQueue) {
      this.registry = registry;
      storageConfig = new StorageConfig("hdfs", registryPath);
      storageConfig.attribute(HDFSStorage.REGISTRY_PATH, registryPath);
      this.tmQueue = tmQueue;
    }
    
    public void run() {
      try {
        doRun();
      } catch (Exception e) {
        logger.error("Error:", e);
      }
    }
    
    void doRun() throws Exception {
      Configuration conf = new Configuration();
      VMConfig.overrideHadoopConfiguration(getVM().getDescriptor().getVmConfig().getHadoopProperties(), conf);
      FileSystem fs = FileSystem.get(conf);
      HDFSStorage storage = new HDFSStorage(registry,fs, storageConfig);

      HDFSSource hdfsSource = storage.getSource();
      HDFSSourcePartition partition = hdfsSource.getLatestSourcePartition();

      BlockingQueue<HDFSSourcePartitionStreamReader> streamReaderQueue = new LinkedBlockingQueue<>();
      HDFSSourcePartitionStream[] stream = partition.getPartitionStreams();
      for(int i = 0; i < stream.length; i++) {
        HDFSSourcePartitionStreamReader reader = stream[i].getReader("validator");
        streamReaderQueue.put(reader);
      }
      
      ExecutorService validatorService = Executors.newFixedThreadPool(1);
      for(int i = 0; i < 3; i++) {
        validatorService.submit(new HDFSPartitionStreamReader(streamReaderQueue, tmQueue));
      }
      validatorService.shutdown();
      while(!validatorService.awaitTermination(5, TimeUnit.MINUTES)) {
        partition.deleteReadDataByActiveReader();
      }
    }
  }
  
  class HDFSPartitionStreamReader implements Runnable {
    private BlockingQueue<HDFSSourcePartitionStreamReader> streamReaderQueue;
    private BlockingQueue<TrackingMessage>                 tmQueue;
    
    HDFSPartitionStreamReader(BlockingQueue<HDFSSourcePartitionStreamReader> queue, BlockingQueue<TrackingMessage> tmQueue) {
      this.streamReaderQueue = queue ;
      this.tmQueue    = tmQueue;
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
      HDFSSourcePartitionStreamReader streamReader = null;
      while((streamReader = streamReaderQueue.poll(10, TimeUnit.MILLISECONDS)) != null) {
        Record record = null;
        while((record = nextWithRetry(streamReader, 1000, 3)) != null) {
          byte[] data = record.getData();
          TrackingMessage tMesg = JSONSerializer.INSTANCE.fromBytes(data, TrackingMessage.class);
          if(!tmQueue.offer(tMesg, 90000, TimeUnit.MILLISECONDS)) {
            throw new Exception("Cannot queue the messages after 5s, increase the buffer");
          }
        }
        streamReader.commit();
        streamReaderQueue.put(streamReader);
      }
    }
    
    Record nextWithRetry(HDFSSourcePartitionStreamReader streamReader, long maxWait, int numOfRetry) throws Exception {
      Exception error = null;
      for(int i = 0; i < numOfRetry; i++) {
        try {
          return streamReader.next(maxWait);
        } catch(Exception ex) {
          error = ex;
          logger.warn("Error when try to read from the HDFSSourcePartitionStreamReader. Rollback and retry. try = " + i, ex);
          streamReader.rollback();
        }
      }
      throw error;
    }
  }
}