package com.neverwinterdp.scribengin.dataflow.tool.tracking;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.scribengin.storage.Record;
import com.neverwinterdp.scribengin.storage.StorageConfig;
import com.neverwinterdp.scribengin.storage.hdfs.HDFSStorage;
import com.neverwinterdp.scribengin.storage.hdfs.source.HDFSSource;
import com.neverwinterdp.scribengin.storage.hdfs.source.HDFSSourcePartition;
import com.neverwinterdp.scribengin.storage.hdfs.source.HDFSSourcePartitionStream;
import com.neverwinterdp.scribengin.storage.hdfs.source.HDFSSourcePartitionStreamReader;
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
    
    String hdfsLocation        = vmConfig.getProperty("hdfs.location", "/tracking-sample/hdfs/aggregate");
    long   partitionRollPeriod = vmConfig.getPropertyAsLong("hdfs.partition-roll-period", (15 * 60 * 1000));
    
    logger.info("reportPath = "          + reportPath);
    logger.info("numOfReader = "         + numOfReader);
    logger.info("maxRuntime = "          + maxRuntime);
    logger.info("hdfsLocation = "        + hdfsLocation);
    logger.info("partitionRollPeriod = " + partitionRollPeriod);
    
    TrackingValidatorService validatorService = new TrackingValidatorService(registry, reportPath);
    validatorService.withExpectNumOfMessagePerChunk(expectNumOfMessagePerChunk);
    validatorService.addReader(
        new HDFSTrackingMessageReader(hdfsLocation, partitionRollPeriod)
    );
    validatorService.start();
    validatorService.awaitForTermination(maxRuntime, TimeUnit.MILLISECONDS);
  }

  public class HDFSTrackingMessageReader extends TrackingMessageReader {
    private BlockingQueue<TrackingMessage> tmQueue = new LinkedBlockingQueue<>(10000);
    private long partitionRollPeriod ;
    private HDFSSourceReader hdfsSourceReader ;
    
    HDFSTrackingMessageReader(String hdfsLocation, long partitionRollPeriod) {
      this.partitionRollPeriod = partitionRollPeriod;
      hdfsSourceReader = new HDFSSourceReader(hdfsLocation, partitionRollPeriod, tmQueue) ;
    }
    
    public void onInit(TrackingRegistry registry) throws Exception {
      hdfsSourceReader.start();
    }
   
    public void onDestroy(TrackingRegistry registry) throws Exception{
      hdfsSourceReader.interrupt();
    }
    
    @Override
    public TrackingMessage next() throws Exception {
      return tmQueue.poll(partitionRollPeriod + 300000, TimeUnit.MILLISECONDS);
    }
  }
  
  public class HDFSSourceReader extends Thread {
    private String hdfsLocation ;
    private long   partitionRollPeriod;
    private BlockingQueue<TrackingMessage> tmQueue;
    
    HDFSSourceReader(String hdfsLocation, long partitionRollPeriod, BlockingQueue<TrackingMessage> tmQueue) {
      this.hdfsLocation        = hdfsLocation;
      this.partitionRollPeriod = partitionRollPeriod;
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
      StorageConfig storageConfig = new StorageConfig("HDFS", hdfsLocation);
      Configuration conf = new Configuration();
      VMConfig.overrideHadoopConfiguration(getVM().getDescriptor().getVmConfig().getHadoopProperties(), conf);
      FileSystem fs = FileSystem.get(conf);

      HDFSStorage hdfsStorage = new HDFSStorage(fs, storageConfig);
      HDFSSource hdfsSource = hdfsStorage.getSource();
      int noPartitionFound = 0 ;
      while(true) {
        List<HDFSSourcePartition> partitions = hdfsSource.getSourcePartitions();
        if(partitions.size() > 0) {
          noPartitionFound = 0;
          for(int i = 0; i < partitions.size(); i++) {
            HDFSSourcePartition partition = partitions.get(i);
            Date timestamp   = getTimestamp(partition.getPartitionLocation());
            Date currentTime = new Date();
            if(currentTime.getTime() > timestamp.getTime() + partitionRollPeriod) {
              validatePartition(partition);
            } else {
              break ;
            }
          }
        } else {
          noPartitionFound++;
        }
        if(noPartitionFound > 50) break ;
        Thread.sleep(15000);
      }
    }
    
    Date getTimestamp(String partitionLocation) throws ParseException {
      SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd-HHmm");
      int index = partitionLocation.indexOf("storage-");
      String timestamp = partitionLocation.substring(index + "storage-".length());
      return timestampFormat.parse(timestamp);
    }
    
    void validatePartition(HDFSSourcePartition partition) throws Exception {
      BlockingQueue<HDFSSourcePartitionStream> streamQueue = new LinkedBlockingQueue<>();
      HDFSSourcePartitionStream[] stream = partition.getPartitionStreams();
      for(int i = 0; i < stream.length; i++) {
        streamQueue.offer(stream[i]);
      }
      ExecutorService service = Executors.newFixedThreadPool(3);
      for(int i = 0; i < stream.length; i++) {
        service.submit(new HDFSPartitionStreamReader(streamQueue, tmQueue));
      }
      service.shutdown();
      service.awaitTermination(2 * partitionRollPeriod, TimeUnit.MILLISECONDS);
      partition.delete();
    }
  }
  
  class HDFSPartitionStreamReader implements Runnable {
    private BlockingQueue<HDFSSourcePartitionStream> streamQueue;
    private BlockingQueue<TrackingMessage>           tmQueue;
    
    HDFSPartitionStreamReader(BlockingQueue<HDFSSourcePartitionStream> streamQueue, BlockingQueue<TrackingMessage> tmQueue) {
      this.streamQueue = streamQueue ;
      this.tmQueue = tmQueue;
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
      HDFSSourcePartitionStream stream = null;
      while((stream = streamQueue.poll(10, TimeUnit.MILLISECONDS)) != null) {
        Record record = null;
        HDFSSourcePartitionStreamReader reader = stream.getReader("validator") ;
        while((record = reader.next(1000)) != null) {
          byte[] data = record.getData();
          TrackingMessage tMesg = JSONSerializer.INSTANCE.fromBytes(data, TrackingMessage.class);
          if(!tmQueue.offer(tMesg, 5000, TimeUnit.MILLISECONDS)) {
            throw new Exception("Cannot queue the messages after 5s, increase the buffer");
          }
        }
        reader.close();
      }
    }
  }
}