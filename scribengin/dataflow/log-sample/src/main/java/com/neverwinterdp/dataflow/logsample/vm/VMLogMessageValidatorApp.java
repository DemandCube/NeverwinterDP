package com.neverwinterdp.dataflow.logsample.vm;

import java.util.concurrent.TimeUnit;

import com.neverwinterdp.kafka.consumer.KafkaMessageConsumerConnector;
import com.neverwinterdp.kafka.consumer.MessageConsumerHandler;
import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.tool.message.Message;
import com.neverwinterdp.tool.message.MessageTracker;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.util.log.Log4jRecord;
import com.neverwinterdp.vm.VMApp;
import com.neverwinterdp.vm.VMConfig;

public class VMLogMessageValidatorApp extends VMApp {
  private MessageTracker messageTracker  ;
  
  @Override
  public void run() throws Exception {
    System.out.println("VMLogValidatorApp: start run()");
    VMConfig vmConfig = getVM().getDescriptor().getVmConfig();
    int numOfExecutor = vmConfig.getPropertyAsInt("num-of-executor", 3);
    long waitForMessageTimeout = vmConfig.getPropertyAsInt("wait-for-message-timeout", 5000);
    long waitForTermination =  vmConfig.getPropertyAsInt("wait-for-termination", 300000);
    String validateTopic =  vmConfig.getProperty("validate-topic");
    String zkConnectUrls = vmConfig.getRegistryConfig().getConnect() ;
    
    messageTracker = new MessageTracker() ;
    KafkaMessageConsumerConnector connector = 
        new KafkaMessageConsumerConnector("LogValidator", zkConnectUrls).
        withConsumerTimeoutMs(waitForMessageTimeout).
        connect();
    MessageConsumerHandler handler = new MessageConsumerHandler() {
      @Override
      public void onMessage(String topic, byte[] key, byte[] message) {
        try {
          Record rec = JSONSerializer.INSTANCE.fromBytes(message, Record.class);
          Log4jRecord log4jRec = JSONSerializer.INSTANCE.fromBytes(rec.getData(), Log4jRecord.class);
          Message lMessage = JSONSerializer.INSTANCE.fromString(log4jRec.getMessage(), Message.class);
          messageTracker.log(lMessage);
        } catch(Throwable t) {
          System.err.println(t.getMessage());
        }
      }
    };
    connector.consume(validateTopic, handler, numOfExecutor);
    connector.awaitTermination(waitForTermination, TimeUnit.MILLISECONDS);
    messageTracker.optimize();
    getVM().getLoggerFactory().getLogger("REPORT").info("\n" + messageTracker.getFormattedReport());
    System.out.println(messageTracker.getFormattedReport());
  }
}