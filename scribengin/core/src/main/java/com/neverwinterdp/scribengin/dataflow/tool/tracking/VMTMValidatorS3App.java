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

import org.slf4j.Logger;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.scribengin.storage.Record;
import com.neverwinterdp.scribengin.storage.StorageConfig;
import com.neverwinterdp.scribengin.storage.s3.S3Client;
import com.neverwinterdp.scribengin.storage.s3.S3Storage;
import com.neverwinterdp.scribengin.storage.s3.source.S3Source;
import com.neverwinterdp.scribengin.storage.s3.source.S3SourcePartition;
import com.neverwinterdp.scribengin.storage.s3.source.S3SourcePartitionStream;
import com.neverwinterdp.scribengin.storage.s3.source.S3SourcePartitionStreamReader;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.vm.VMApp;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;

public class VMTMValidatorS3App extends VMApp {
  private Logger logger;
  
  @Override
  public void run() throws Exception {
    try {
    logger =  getVM().getLoggerFactory().getLogger(VMTMValidatorS3App.class) ;
    logger.info("Start run()");
    System.err.println("VMTMValidatorS3App: start run");
    VMDescriptor vmDescriptor = getVM().getDescriptor();
    VMConfig vmConfig = vmDescriptor.getVmConfig();
    Registry registry = getVM().getVMRegistry().getRegistry();
    registry.setRetryable(true);
    
    String reportPath   = vmConfig.getProperty("tracking.report-path", "/applications/tracking-message");
    int    numOfReader  = vmConfig.getPropertyAsInt("tracking.num-of-reader", 3);
    long   maxRuntime   = vmConfig.getPropertyAsLong("tracking.max-runtime", 120000);
    int    expectNumOfMessagePerChunk = vmConfig.getPropertyAsInt("tracking.expect-num-of-message-per-chunk", 0);
    
    String s3BucketName        = vmConfig.getProperty("s3.bucket.name", "tracking-sample-bucket");
    String s3StoragePath       = vmConfig.getProperty("s3.storage.path", "tracking-sample");
    long   partitionRollPeriod = vmConfig.getPropertyAsLong("s3.partition-roll-period", (15 * 60 * 1000));
    
    logger.info("reportPath = "          + reportPath);
    logger.info("numOfReader = "         + numOfReader);
    logger.info("maxRuntime = "          + maxRuntime);
    logger.info("s3.bucket.name  = "      + s3BucketName);
    logger.info("s3.storage.path = "      + s3StoragePath);
    logger.info("partitionRollPeriod = " + partitionRollPeriod);
    
    TrackingValidatorService validatorService = new TrackingValidatorService(registry, reportPath);
    validatorService.withExpectNumOfMessagePerChunk(expectNumOfMessagePerChunk);
    validatorService.addReader(
        new S3TrackingMessageReader(s3BucketName, s3StoragePath, partitionRollPeriod)
    );
    validatorService.start();
    validatorService.awaitForTermination(maxRuntime, TimeUnit.MILLISECONDS);
    } catch(Throwable t) {
      t.printStackTrace();
    }
  }

  public class S3TrackingMessageReader extends TrackingMessageReader {
    private long           partitionRollPeriod ;
    private S3SourceConnector s3SourceConnector ;
    private BlockingQueue<TrackingMessage> tmQueue = new LinkedBlockingQueue<>(5000);
    
    S3TrackingMessageReader(String bucketName, String storagePath, long partitionRollPeriod) {
      this.partitionRollPeriod = partitionRollPeriod;
      s3SourceConnector = new S3SourceConnector(bucketName, storagePath, partitionRollPeriod, tmQueue) ;
    }
    
    public void onInit(TrackingRegistry registry) throws Exception {
      s3SourceConnector.start();
    }
   
    public void onDestroy(TrackingRegistry registry) throws Exception{
      s3SourceConnector.interrupt();
    }
    
    @Override
    public TrackingMessage next() throws Exception {
      return tmQueue.poll(partitionRollPeriod + 300000, TimeUnit.MILLISECONDS);
    }
  }
  
  public class S3SourceConnector extends Thread {
    private String bucketName;
    private String storagePath;
    private long   partitionRollPeriod;
    private BlockingQueue<TrackingMessage> tmQueue;
    
    S3SourceConnector(String bucketName, String storagePath, long partitionRollPeriod, BlockingQueue<TrackingMessage> tmQueue) {
      this.bucketName          = bucketName;
      this.storagePath         = storagePath;
      this.partitionRollPeriod = partitionRollPeriod;
      this.tmQueue             = tmQueue;
    }
    
    public void run() {
      try {
        doRun();
      } catch (Exception e) {
        logger.error("Error:", e);
      }
    }
    
    void doRun() throws Exception {
      StorageConfig storageConfig = new StorageConfig("s3", bucketName + ":" + storagePath);
      storageConfig.attribute(S3Storage.BUCKET_NAME, bucketName);
      storageConfig.attribute(S3Storage.STORAGE_PATH, storagePath);
      storageConfig.setPartitionStream(8);
      S3Client s3Client = new S3Client();

      S3Storage s3Storage = new S3Storage(s3Client, storageConfig);
      S3Source  s3Source = s3Storage.getSource();
      int noPartitionFound = 0 ;
      while(true) {
        List<S3SourcePartition> partitions = s3Source.getSourcePartitions();
        if(partitions.size() > 0) {
          noPartitionFound = 0;
          for(int i = 0; i < partitions.size(); i++) {
            S3SourcePartition partition = partitions.get(i);
            Date timestamp   = getTimestamp(partition.getPartitionName());
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
    
    Date getTimestamp(String partitionName) throws ParseException {
      SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd-HHmm");
      int index = partitionName.indexOf("storage-");
      String timestamp = partitionName.substring(index + "storage-".length());
      return timestampFormat.parse(timestamp);
    }
    
    void validatePartition(S3SourcePartition partition) throws Exception {
      BlockingQueue<S3SourcePartitionStream> streamQueue = new LinkedBlockingQueue<>();
      S3SourcePartitionStream[] stream = partition.getPartitionStreams();
      for(int i = 0; i < stream.length; i++) {
        streamQueue.offer(stream[i]);
      }
      ExecutorService service = Executors.newFixedThreadPool(stream.length);
      for(int i = 0; i < stream.length; i++) {
        service.submit(new S3PartitionStreamReader(streamQueue, tmQueue));
      }
      service.shutdown();
      service.awaitTermination(2 * partitionRollPeriod, TimeUnit.MILLISECONDS);
      partition.delete();
    }
  }
  
  class S3PartitionStreamReader implements Runnable {
    private BlockingQueue<S3SourcePartitionStream> streamQueue;
    private BlockingQueue<TrackingMessage>         tmQueue;
    
    S3PartitionStreamReader(BlockingQueue<S3SourcePartitionStream> streamQueue, BlockingQueue<TrackingMessage> tmQueue) {
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
      S3SourcePartitionStream stream = null;
      while((stream = streamQueue.poll(10, TimeUnit.MILLISECONDS)) != null) {
        Record record = null;
        S3SourcePartitionStreamReader reader = stream.getReader("validator") ;
        while((record = reader.next(1000)) != null) {
          byte[] data = record.getData();
          TrackingMessage tMesg = JSONSerializer.INSTANCE.fromBytes(data, TrackingMessage.class);
          tmQueue.offer(tMesg);
        }
        reader.close();
      }
    }
  }
}