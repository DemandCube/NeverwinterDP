package com.neverwinterdp.dataflow.logsample.vm;

import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.neverwinterdp.dataflow.logsample.LogMessageReport;
import com.neverwinterdp.dataflow.logsample.LogSampleRegistry;
import com.neverwinterdp.kafka.producer.AckKafkaWriter;
import com.neverwinterdp.kafka.tool.KafkaTool;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.tool.message.MessageGenerator;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.util.log.Log4jRecord;
import com.neverwinterdp.vm.VMApp;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;

public class VMLogMessageGeneratorApp extends VMApp {
  private Logger logger ;
  int numOfMessagePerExecutor ;
  int messageSize;
  String zkConnects ;
  String topic ;
  
  @Override
  public void run() throws Exception {
    logger = getVM().getLoggerFactory().getLogger(VMLogMessageGeneratorApp.class);
    VMDescriptor vmDescriptor = getVM().getDescriptor();
    VMConfig vmConfig = vmDescriptor.getVmConfig();
    int numOfExecutor = vmConfig.getPropertyAsInt("num-of-executor", 1);
    zkConnects = vmConfig.getProperty("zk-connects", "zookeeper-1:2181");
    topic      = vmConfig.getProperty("topic", "log4j");
    numOfMessagePerExecutor = vmConfig.getPropertyAsInt("num-of-message-per-executor", 5000);
    messageSize = vmConfig.getPropertyAsInt("message-size", 256);
    
    ExecutorService executorService = Executors.newFixedThreadPool(numOfExecutor);

    for(int i = 0; i < numOfExecutor; i++) {
      String vmId = getVM().getDescriptor().getId();
      String groupId = vmId + "-executor-" + (i + 1);
      executorService.submit(new RunnableLogMessageGenerator(groupId));
    }
    executorService.shutdown();
    executorService.awaitTermination(60, TimeUnit.MINUTES);
  }
  
  public class RunnableLogMessageGenerator implements Runnable {
    private String groupId ;
    
    public  RunnableLogMessageGenerator(String groupId) {
      this.groupId = groupId;
    }
    
    @Override
    public void run() {
      MessageGenerator messageGenerator = new MessageGenerator.DefaultMessageGenerator() ;
      try {
        KafkaLogWriter logWriter = new KafkaLogWriter(zkConnects, topic);
        for(int i = 0; i < numOfMessagePerExecutor; i++) {
          int mod = i % 3 ;
          String jsonMessage = new String(messageGenerator.nextMessage(groupId, messageSize)) ;
          if(mod == 0) logWriter.write("LogSample", "ERROR", jsonMessage);
          else if (mod == 1) logWriter.write("LogSample", "WARN", jsonMessage);
          else logWriter.write("LogSample", "INFO", jsonMessage);
        }
        logWriter.close();
      } catch(Throwable t) {
        logger.error("Generate message error", t);
      }
      LogSampleRegistry appRegistry = null;
      try {
        appRegistry = new LogSampleRegistry(getVM().getVMRegistry().getRegistry(), true);
        LogMessageReport report = new LogMessageReport(groupId, messageGenerator.getCurrentSequenceId(groupId), 0, 0) ;
        appRegistry.addGenerateReport(report);
      } catch (RegistryException e) {
        if(appRegistry != null) {
          try {
            appRegistry.addGenerateError(groupId, e);
          } catch (RegistryException error) {
            logger.error("Log info to registry error", error) ;
          }
        }
        logger.error("Log info to registry error", e) ;
      }
    }
  }
  
  static public class KafkaLogWriter {
    private String zkConnects ;
    private String topic ;
    private AckKafkaWriter kafkaWriter;
    
    public KafkaLogWriter(String zkConnects, String topic) throws Exception {
      this.zkConnects = zkConnects ;
      this.topic = topic ;
      
      KafkaTool kafkaTool = new KafkaTool("KafkaTool", zkConnects);
      kafkaTool.connect();
      String kafkaConnects = kafkaTool.getKafkaBrokerList();
      kafkaTool.close();
      kafkaWriter = new AckKafkaWriter("KafkaLogWriter", kafkaConnects) ;
      kafkaWriter.reconnect();
    }
    
    public void write(String loggerName, String level, String message) {
      Log4jRecord record = new Log4jRecord();
      record.setTimestamp(new Date());
      record.setLoggerName(loggerName);
      record.setLevel(level);
      record.setMessage(message);
      write(record);
    }
    
    public void write(Log4jRecord record)  {
      String json = JSONSerializer.INSTANCE.toString(record);
      try {
        kafkaWriter.send(topic, json, 60 * 1000);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    
    public void close() {
      kafkaWriter.waitAndClose(150000);
    }
  }
}