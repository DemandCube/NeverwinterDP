package com.neverwinterdp.dataflow.logsample.vm;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.neverwinterdp.dataflow.logsample.LogMessageReport;
import com.neverwinterdp.dataflow.logsample.LogSampleRegistry;
import com.neverwinterdp.kafka.consumer.KafkaMessageConsumerConnector;
import com.neverwinterdp.kafka.consumer.MessageConsumerHandler;
import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.hdfs.source.HDFSSource;
import com.neverwinterdp.scribengin.storage.source.SourceStream;
import com.neverwinterdp.scribengin.storage.source.SourceStreamReader;
import com.neverwinterdp.tool.message.BitSetMessageTracker;
import com.neverwinterdp.tool.message.Message;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.util.log.Log4jRecord;
import com.neverwinterdp.vm.VMApp;
import com.neverwinterdp.vm.VMConfig;

public class VMLogMessageValidatorApp extends VMApp {
  int  numOfMessagePerPartition ;
  long waitForTermination ;
  String validateKafkaTopic ;
  String validateHdfs ;
  
  BitSetMessageTracker bitSetMessageTracker;
  
  @Override
  public void run() throws Exception {
    System.out.println("VMLogValidatorApp: start run()");
    VMConfig vmConfig = getVM().getDescriptor().getVmConfig();
    numOfMessagePerPartition = vmConfig.getPropertyAsInt("num-of-message-per-partition", -1);
    waitForTermination       = vmConfig.getPropertyAsInt("wait-for-termination", 300000);
    
    validateKafkaTopic =  vmConfig.getProperty("validate-kafka", null);
    validateHdfs =  vmConfig.getProperty("validate-hdfs", null);
    bitSetMessageTracker = new BitSetMessageTracker(numOfMessagePerPartition);
    
    System.out.println("validate hdfs: " + validateHdfs);
    if(validateKafkaTopic != null) {
      KafkaMessageValidator kafkaValidator = new KafkaMessageValidator() ;
      kafkaValidator.validate(validateKafkaTopic);
    } else if(validateHdfs != null) {
      HDFSMessageValidator hdfsValidator = new HDFSMessageValidator() ;
      String[] hdfsLoc = validateHdfs.split(",");
      for(String selHdfsLoc : hdfsLoc) {
        hdfsValidator.validate(selHdfsLoc);
      }
    }
    
    report(bitSetMessageTracker);
    String formattedReport = bitSetMessageTracker.getFormatedReport();
    System.out.println(formattedReport);
    getVM().getLoggerFactory().getLogger("REPORT").info(formattedReport);
  }
  
  void report(BitSetMessageTracker tracker) throws Exception {
    LogSampleRegistry appRegistry = null;
    appRegistry = new LogSampleRegistry(getVM().getVMRegistry().getRegistry(), true);
    for(String partition : tracker.getPartitions()) {
      BitSetMessageTracker.BitSetPartitionMessageTracker pTracker = tracker.getPartitionTracker(partition);
      LogMessageReport report = new LogMessageReport(partition, pTracker.getExpect(), pTracker.getLostCount(), pTracker.getDuplicatedCount()) ;
      appRegistry.addValidateReport(report);
    }
  }
  
  public class HDFSMessageValidator {
    public void validate(String hdfsLoc) throws Exception {
      Configuration conf = new Configuration();
      getVM().getDescriptor().getVmConfig().getHadoopProperties().overrideConfiguration(conf);;
      FileSystem fs = FileSystem.get(conf);
      StorageDescriptor storageDescriptor = new StorageDescriptor("HDFS", hdfsLoc) ;
      HDFSSource source = new HDFSSource(fs, storageDescriptor) ;
      SourceStream[] streams = source.getStreams();
      for(SourceStream selStream : streams) {
        SourceStreamReader streamReader = selStream.getReader("HDFSSinkValidator") ;
        Record record = null ;
        while((record = streamReader.next(5000)) != null) {
          Log4jRecord log4jRec = JSONSerializer.INSTANCE.fromBytes(record.getData(), Log4jRecord.class);
          Message lMessage = JSONSerializer.INSTANCE.fromString(log4jRec.getMessage(), Message.class);
          bitSetMessageTracker.log(lMessage.getPartition(), lMessage.getTrackId());
        }
        streamReader.close();
      }
    }
  }
  
  public class KafkaMessageValidator {
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
            Record rec = JSONSerializer.INSTANCE.fromBytes(message, Record.class);
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