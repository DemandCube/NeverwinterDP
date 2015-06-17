package com.neverwinterdp.dataflow.logsample.vm;

import java.util.concurrent.atomic.AtomicInteger;

import com.neverwinterdp.dataflow.logsample.LogMessage;
import com.neverwinterdp.kafka.consumer.KafkaMessageConsumerConnector;
import com.neverwinterdp.kafka.consumer.MessageConsumerHandler;
import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.util.log.Log4jRecord;
import com.neverwinterdp.vm.VMApp;

public class VMLogMessageValidatorApp extends VMApp {
  
  @Override
  public void run() throws Exception {
    System.out.println("VMLogValidatorApp: start run()");
    String zkConnectUrls = getVM().getDescriptor().getVmConfig().getRegistryConfig().getConnect() ;
    KafkaMessageConsumerConnector connector = new KafkaMessageConsumerConnector("LogValidator", zkConnectUrls) ;
    final AtomicInteger counter = new AtomicInteger();
    MessageConsumerHandler handler = new MessageConsumerHandler() {
      @Override
      public void onMessage(String topic, byte[] key, byte[] message) {
        try {
          Record rec = JSONSerializer.INSTANCE.fromBytes(message, Record.class);
          Log4jRecord log4jRec = JSONSerializer.INSTANCE.fromBytes(rec.getData(), Log4jRecord.class);
          LogMessage lMessage = JSONSerializer.INSTANCE.fromString(log4jRec.getMessage(), LogMessage.class);
          counter.incrementAndGet();
        } catch(Throwable t) {
          System.err.println(t.getMessage());
        }
      }
    };
    connector.consume("log4j.aggregate", handler, 3);
    Thread.sleep(5000);
    System.out.println("VMLogValidatorApp: finish run(), count = " + counter.get());
  }
}