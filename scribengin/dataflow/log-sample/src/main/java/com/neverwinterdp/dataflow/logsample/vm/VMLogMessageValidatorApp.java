package com.neverwinterdp.dataflow.logsample.vm;

import java.util.concurrent.TimeUnit;

import com.neverwinterdp.dataflow.logsample.LogMessageReport;
import com.neverwinterdp.dataflow.logsample.LogSampleRegistry;
import com.neverwinterdp.kafka.consumer.KafkaMessageConsumerConnector;
import com.neverwinterdp.kafka.consumer.MessageConsumerHandler;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.tool.message.BitSetMessageTracker;
import com.neverwinterdp.tool.message.Message;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.util.log.Log4jRecord;
import com.neverwinterdp.vm.VMApp;
import com.neverwinterdp.vm.VMConfig;

public class VMLogMessageValidatorApp extends VMApp {
  BitSetMessageTracker bitSetMessageTracker;
  
  @Override
  public void run() throws Exception {
    System.out.println("VMLogValidatorApp: start run()");
    VMConfig vmConfig = getVM().getDescriptor().getVmConfig();
    int numOfMessagePerPartition = vmConfig.getPropertyAsInt("num-of-message-per-partition", -1);
    int numOfExecutor = vmConfig.getPropertyAsInt("num-of-executor", 3);
    long waitForMessageTimeout = vmConfig.getPropertyAsInt("wait-for-message-timeout", 5000);
    long waitForTermination =  vmConfig.getPropertyAsInt("wait-for-termination", 300000);
    String validateTopic =  vmConfig.getProperty("validate-topic");
    String zkConnectUrls = vmConfig.getRegistryConfig().getConnect() ;
    
    KafkaMessageConsumerConnector connector = 
        new KafkaMessageConsumerConnector("LogValidator", zkConnectUrls).
        withConsumerTimeoutMs(waitForMessageTimeout).
        connect();
    final BitSetMessageTracker bitSetMessageTracker = new BitSetMessageTracker(numOfMessagePerPartition);
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
    connector.consume(validateTopic, handler, numOfExecutor);
    try {
      connector.awaitTermination(waitForTermination, TimeUnit.MILLISECONDS);
      report(bitSetMessageTracker);
      String formattedReport = bitSetMessageTracker.getFormatedReport();
      System.out.println(formattedReport);
      getVM().getLoggerFactory().getLogger("REPORT").info(formattedReport);
      
    } catch(Exception ex) {
      getVM().getLoggerFactory().getLogger("REPORT").error("Error for waiting validation", ex);
    }
  }
  
  private void report(BitSetMessageTracker tracker) throws Exception {
    LogSampleRegistry appRegistry = null;
    try {
      appRegistry = new LogSampleRegistry(getVM().getVMRegistry().getRegistry(), true);
      for(String partition : tracker.getPartitions()) {
        BitSetMessageTracker.BitSetPartitionMessageTracker pTracker = tracker.getPartitionTracker(partition);
        LogMessageReport report = new LogMessageReport(partition, pTracker.getExpect(), pTracker.getLostCount(), pTracker.getDuplicatedCount()) ;
        appRegistry.addValidateReport(report);
      }
    } catch (RegistryException e) {
      e.printStackTrace();
    }
  }
}