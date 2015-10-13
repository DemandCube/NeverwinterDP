package com.neverwinterdp.dataflow.logsample.vm;

import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.neverwinterdp.kafka.producer.AckKafkaWriter;
import com.neverwinterdp.kafka.tool.KafkaTool;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.scribengin.dataflow.tool.tracking.TrackingReport;
import com.neverwinterdp.scribengin.dataflow.tool.tracking.TrackingRegistry;
import com.neverwinterdp.tool.message.MessageGenerator;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.util.log.Log4jRecord;
import com.neverwinterdp.vm.VMApp;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;

public class VMToKafkaLogMessageGeneratorApp extends VMApp {
  private Logger logger ;
  private int numOfMessage ;
  private int messageSize;
  private long sendPeriod ;
  private String reportPath ;
  private String zkConnects ;
  private String topic ;
  private MessageGenerator messageGenerator = new MessageGenerator.DefaultMessageGenerator() ;
  private int messageGeneratorCount ;
  private TrackingRegistry appRegistry;
  
  @Override
  public void run() throws Exception {
    logger = getVM().getLoggerFactory().getLogger(VMToKafkaLogMessageGeneratorApp.class);
    VMDescriptor vmDescriptor = getVM().getDescriptor();
    VMConfig vmConfig = vmDescriptor.getVmConfig();
    reportPath = vmConfig.getProperty("report-path", "/applications/log-sample");
    zkConnects = vmConfig.getProperty("zk-connects", "zookeeper-1:2181");
    topic      = vmConfig.getProperty("topic", "log4j");
    numOfMessage = vmConfig.getPropertyAsInt("num-of-message", 5000);
    messageSize = vmConfig.getPropertyAsInt("message-size", 256);
    int numOfStream = vmConfig.getPropertyAsInt("num-of-stream", 10);
    sendPeriod = vmConfig.getPropertyAsLong("send-period", -1);

    Registry registry = getVM().getVMRegistry().getRegistry();
    appRegistry = new TrackingRegistry(registry, reportPath, true);
    String vmId = getVM().getDescriptor().getId();
    appRegistry.addGenerateReport(new TrackingReport(vmId, numOfMessage, messageGenerator.getCurrentSequenceId(vmId), 0, 0));
    
    
    KafkaTool kafkaTool = new KafkaTool("KafkaTool", zkConnects);
    if(!kafkaTool.topicExits(topic)) kafkaTool.createTopic(topic, 1, numOfStream);
    
    TopicMetadata topicMetadata = kafkaTool.findTopicMetadata(topic);
    List<PartitionMetadata> partitionMetadataHolder = topicMetadata.partitionsMetadata();
    ExecutorService executorService = Executors.newFixedThreadPool(partitionMetadataHolder.size());
    for(int i = 0; i < partitionMetadataHolder.size(); i++) {
      executorService.submit(new RunnableLogMessageGenerator(partitionMetadataHolder.get(i)));
    }
    executorService.shutdown();
    //Hard coded to run for a week at maximum
    executorService.awaitTermination(7*24*60, TimeUnit.MINUTES);
    
    TrackingReport finishReport = new TrackingReport(vmId, numOfMessage, messageGenerator.getCurrentSequenceId(vmId), 0, 0);
    appRegistry.updateGenerateReport(finishReport);
    System.out.println("LOG GENERATOR:");
    System.out.println("Report Path: " + reportPath);
    System.out.println(JSONSerializer.INSTANCE.toString(finishReport));
  }
  
  synchronized private String nextMessage() {
    if(messageGeneratorCount >= this.numOfMessage) return null ;
    String vmId = getVM().getDescriptor().getId();
    String jsonMessage = new String(messageGenerator.nextMessage(vmId, messageSize)) ;
    messageGeneratorCount++ ;
    return jsonMessage;
  }
  
  public class RunnableLogMessageGenerator implements Runnable {
    private PartitionMetadata partitionMetadata;
    
    public RunnableLogMessageGenerator(PartitionMetadata partitionMetadata) {
      this.partitionMetadata = partitionMetadata ;
    }
    
    @Override
    public void run() {
      logger.info("Start generate message for partition " + partitionMetadata.partitionId()); 
      try {
        KafkaPartitionLogWriter logWriter = new KafkaPartitionLogWriter(zkConnects, topic, partitionMetadata);
        String jsonMessage = null ;
        int count = 0 ;
        while((jsonMessage = nextMessage()) != null) {
          count++ ;
          int mod = count % 3 ;
          if(mod == 0) logWriter.write("LogSample", "ERROR", jsonMessage);
          else if (mod == 1) logWriter.write("LogSample", "WARN", jsonMessage);
          else logWriter.write("LogSample", "INFO", jsonMessage);
          
          String vmId = getVM().getDescriptor().getId();
          int currentSeqId = messageGenerator.getCurrentSequenceId(vmId);
          if(count % 500000 == 0) {
            TrackingReport finishReport = new TrackingReport(vmId, currentSeqId, currentSeqId, 0, 0);
            appRegistry.updateGenerateReport(finishReport);
          }
          if(sendPeriod > 0) {
            Thread.sleep(sendPeriod);
          }
        }
        logWriter.write("LogSample", "INFO",  "EOS");
        logWriter.close();
        logger.info("Finish generate message for partition " + partitionMetadata.partitionId() + ", num of message = " + numOfMessage); 
      } catch(Throwable t) {
        logger.error("Generate message error", t);
      }
    }
  }
  
  public class KafkaPartitionLogWriter {
    private String topic ;
    private PartitionMetadata partitionMetadata ;
    private AckKafkaWriter kafkaWriter;
    
    public KafkaPartitionLogWriter(String zkConnects, String topic, PartitionMetadata partitionMetadata) throws Exception {
      this.topic = topic ;
      this.partitionMetadata = partitionMetadata;
      KafkaTool kafkaTool = new KafkaTool("KafkaTool", zkConnects);
      String kafkaConnects = kafkaTool.getKafkaBrokerList();
      kafkaWriter = new AckKafkaWriter("KafkaLogWriter", kafkaConnects) ;
      kafkaWriter.reconnect();
    }
    
    public void write(String loggerName, String level, String message) throws Exception {
      Log4jRecord record = new Log4jRecord();
      record.setTimestamp(new Date());
      record.setLoggerName(loggerName);
      record.setLevel(level);
      record.setMessage(message);
      write(record);
    }
    
    public void write(Log4jRecord record) throws Exception  {
      String json = JSONSerializer.INSTANCE.toString(record);
      kafkaWriter.send(topic, partitionMetadata.partitionId(), json, 60 * 1000);
    }
    
    public void close() {
      kafkaWriter.waitAndClose(150000);
    }
  }
}
