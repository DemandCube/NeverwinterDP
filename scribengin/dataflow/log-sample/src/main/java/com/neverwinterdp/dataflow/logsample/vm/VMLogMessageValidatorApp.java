package com.neverwinterdp.dataflow.logsample.vm;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;

import com.neverwinterdp.dataflow.logsample.MessageReport;
import com.neverwinterdp.dataflow.logsample.MessageReportRegistry;
import com.neverwinterdp.kafka.consumer.KafkaMessageConsumerConnector;
import com.neverwinterdp.kafka.consumer.MessageConsumerHandler;
import com.neverwinterdp.scribengin.dataflow.DataflowMessage;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.hdfs.source.HDFSSource;
import com.neverwinterdp.scribengin.storage.s3.S3Storage;
import com.neverwinterdp.scribengin.storage.s3.source.S3Source;
import com.neverwinterdp.scribengin.storage.source.SourceStream;
import com.neverwinterdp.scribengin.storage.source.SourceStreamReader;
import com.neverwinterdp.tool.message.BitSetMessageTracker;
import com.neverwinterdp.tool.message.Message;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.util.log.Log4jRecord;
import com.neverwinterdp.vm.VMApp;
import com.neverwinterdp.vm.VMConfig;

public class VMLogMessageValidatorApp extends VMApp {
  private Logger logger ;
  int  numOfMessagePerPartition ;
  long waitForTermination ;
  String reportPath ;
  String validateKafkaTopic ;
  String validateHdfs ;
  String validateS3 ;
  
  BitSetMessageTracker bitSetMessageTracker;
  
  @Override
  public void run() throws Exception {
    logger =  getVM().getLoggerFactory().getLogger(VMLogMessageValidatorApp.class) ;
    logger.info("Start run()");
    VMConfig vmConfig = getVM().getDescriptor().getVmConfig();
    numOfMessagePerPartition = vmConfig.getPropertyAsInt("num-of-message-per-partition", -1);
    waitForTermination       = vmConfig.getPropertyAsInt("wait-for-termination", 300000);
    
    reportPath = vmConfig.getProperty("report-path", "/apps/log-sample/reports");
    
    validateKafkaTopic =  vmConfig.getProperty("validate-kafka", null);
    validateHdfs =  vmConfig.getProperty("validate-hdfs", null);
    validateS3 =  vmConfig.getProperty("validate-s3", null);
    
    bitSetMessageTracker = new BitSetMessageTracker(numOfMessagePerPartition);
    
    if(validateKafkaTopic != null) {
      String[] topic = validateKafkaTopic.split(",");
      ExecutorService executorService = Executors.newFixedThreadPool(topic.length);
      for(String selTopic : topic) {
        executorService.submit(new KafkaMessageValidator(selTopic) );
      }
      executorService.shutdown();
      executorService.awaitTermination(waitForTermination, TimeUnit.MILLISECONDS);
    } else if(validateHdfs != null) {
      String[] hdfsLoc = validateHdfs.split(",");
      ExecutorService executorService = Executors.newFixedThreadPool(hdfsLoc.length);
      for(String selHdfsLoc : hdfsLoc) {
        executorService.submit(new HDFSMessageValidator(selHdfsLoc));
      }
      executorService.shutdown();
      executorService.awaitTermination(waitForTermination + 90000, TimeUnit.MILLISECONDS);
    } else if(validateS3 != null) {
      String[] s3Loc = validateS3.split(",");
      ExecutorService executorService = Executors.newFixedThreadPool(s3Loc.length);
      for(String selS3Loc : s3Loc) {
        int colonIdx = selS3Loc.indexOf(":");
        String bucketName = selS3Loc.substring(0, colonIdx);
        String folderPath = selS3Loc.substring(colonIdx + 1) ;
        executorService.submit(new S3MessageValidator(bucketName, folderPath)) ;
      }
      executorService.shutdown();
      executorService.awaitTermination(waitForTermination + 90000, TimeUnit.MILLISECONDS);
    }
    
    report(bitSetMessageTracker);
    String formattedReport = bitSetMessageTracker.getFormatedReport();
    System.out.println(formattedReport);
    logger.info("\n" + formattedReport);
  }
  
  void report(BitSetMessageTracker tracker) throws Exception {
    MessageReportRegistry appRegistry = null;
    appRegistry = new MessageReportRegistry(getVM().getVMRegistry().getRegistry(),reportPath, true);
    for(String partition : tracker.getPartitions()) {
      BitSetMessageTracker.BitSetPartitionMessageTracker pTracker = tracker.getPartitionTracker(partition);
      MessageReport report = new MessageReport(partition, pTracker.getExpect(), pTracker.getLostCount(), pTracker.getDuplicatedCount()) ;
      appRegistry.addValidateReport(report);
    }
  }
  
  public class HDFSMessageValidator implements Runnable {
    String hdfsLoc;
    
    HDFSMessageValidator(String hdfsLoc) {
      this.hdfsLoc = hdfsLoc;
    }
    
    public void run() {
      try {
        validate() ;
      } catch (Exception e) {
        logger.error("Validate HDFS Source error, hdfs loc=" + hdfsLoc, e) ;
      }
    }
    
    public void validate() throws Exception {
      Configuration conf = new Configuration();
      getVM().getDescriptor().getVmConfig().getHadoopProperties().overrideConfiguration(conf);;
      FileSystem fs = FileSystem.get(conf);
      StorageDescriptor storageDescriptor = new StorageDescriptor("HDFS", hdfsLoc) ;
      HDFSSource source = new HDFSSource(fs, storageDescriptor) ;
      SourceStream[] streams = source.getStreams();
      ExecutorService executorService = Executors.newFixedThreadPool(streams.length);
      for(final SourceStream selStream : streams) {
        Runnable runnable = new Runnable() {
          @Override
          public void run() {
            try {
              SourceStreamReader streamReader = selStream.getReader("HDFSSinkValidator") ;
              DataflowMessage dflMessage = null ;
              while((dflMessage = streamReader.next(5000)) != null) {
                Log4jRecord log4jRec = JSONSerializer.INSTANCE.fromBytes(dflMessage.getData(), Log4jRecord.class);
                Message lMessage = JSONSerializer.INSTANCE.fromString(log4jRec.getMessage(), Message.class);
                bitSetMessageTracker.log(lMessage.getPartition(), lMessage.getTrackId());
              }
              streamReader.close();
            } catch(Exception ex) {
              logger.error("Validate HDFS Source Stream Error:", ex) ;
            }
          }
        };
        executorService.submit(runnable);
      }
      executorService.shutdown();
      executorService.awaitTermination(waitForTermination, TimeUnit.MILLISECONDS);
    }
  }
  
  public class S3MessageValidator implements Runnable {
    String bucketName;
    String storageFolder;
    
    public S3MessageValidator(String bucketName, String storageFolder) {
      this.bucketName = bucketName;
      this.storageFolder = storageFolder;
    }
    
    public void run() {
      try {
        validate() ;
      } catch (Exception e) {
        logger.error("Validate S3 Source error, bucketName=" + bucketName + ", folder = " + storageFolder, e) ;
      }
    }
    
    public void validate() throws Exception {
      Configuration conf = new Configuration();
      getVM().getDescriptor().getVmConfig().getHadoopProperties().overrideConfiguration(conf);;

      S3Storage storage = new S3Storage(bucketName, storageFolder);
      S3Source source = storage.getSource();
      SourceStream[] streams = source.getStreams();
      ExecutorService executorService = Executors.newFixedThreadPool(streams.length);
      for (int i = 0; i < streams.length; i++) {
        final SourceStream stream = streams[i];
        Runnable runnable = new Runnable() {
          @Override
          public void run() {
            try {
              SourceStreamReader streamReader = stream.getReader("HDFSSinkValidator") ;
              DataflowMessage dflMessage = null ;
              while((dflMessage = streamReader.next(5000)) != null) {
                Log4jRecord log4jRec = JSONSerializer.INSTANCE.fromBytes(dflMessage.getData(), Log4jRecord.class);
                Message lMessage = JSONSerializer.INSTANCE.fromString(log4jRec.getMessage(), Message.class);
                bitSetMessageTracker.log(lMessage.getPartition(), lMessage.getTrackId());
              }
              streamReader.close();
            } catch(Exception ex) {
              logger.error("Validate HDFS Source Stream Error:", ex) ;
            }
          }
        };
        executorService.submit(runnable);
      }
      executorService.shutdown();
      executorService.awaitTermination(waitForTermination, TimeUnit.MILLISECONDS);
    }
  }
  
  public class KafkaMessageValidator implements Runnable {
    private String topic ;
    
    KafkaMessageValidator(String topic) {
      this.topic = topic;
    }
    
    
    public void run() {
      validate(topic);
    }
    
    public void validate(String topic) {
      String zkConnectUrls = getVM().getDescriptor().getVmConfig().getRegistryConfig().getConnect() ;
      KafkaMessageConsumerConnector connector = 
          new KafkaMessageConsumerConnector("LogValidator", zkConnectUrls).
          withConsumerTimeoutMs(10000).
          connect();
      MessageConsumerHandler handler = new MessageConsumerHandler() {
        @Override
        public void onMessage(String topic, byte[] key, byte[] message) {
          try {
            DataflowMessage rec = JSONSerializer.INSTANCE.fromBytes(message, DataflowMessage.class);
            Log4jRecord log4jRec = JSONSerializer.INSTANCE.fromBytes(rec.getData(), Log4jRecord.class);
            Message lMessage = JSONSerializer.INSTANCE.fromString(log4jRec.getMessage(), Message.class);
            //messageTracker.log(lMessage);
            bitSetMessageTracker.log(lMessage.getPartition(), lMessage.getTrackId());
          } catch(Throwable t) {
            System.err.println(t.getMessage());
          }
        }
      };
      try {
        connector.consume(topic, handler, 3 /*numOfThread*/);
        connector.awaitTermination(waitForTermination, TimeUnit.MILLISECONDS);
      } catch(Exception ex) {
        getVM().getLoggerFactory().getLogger("REPORT").error("Error for waiting validation", ex);
      }
    }
  }
}