package com.neverwinterdp.scribengin.dataflow.api;

import org.junit.Test;

import com.neverwinterdp.storage.kafka.KafkaStorageConfig;
import com.neverwinterdp.util.JSONSerializer;

public class APIUnitTest {
  
  @Test
  public void testApi() throws Exception {
    Dataflow<Message, Message> dfl = new Dataflow<Message, Message>("dataflow");
    dfl.useWireDataStreamFactory(new KafkaWireDataStreamFactory("127.0.0.1:2181"));
    KafkaDataStream<Message> inputDs     = 
        dfl.createInput(new KafkaStorageConfig("input", "127.0.0.1:2181", "input"));
    KafkaDataStream<Message> aggregateDs = 
        dfl.createOutput(new KafkaStorageConfig("aggregate", "127.0.0.1:2181", "aggregate"));
   
    Operator<Message, Message> splitterOp = dfl.createOperator("splitter");
    Operator<Message, Message> infoOp  = dfl.createOperator("info");
    Operator<Message, Message> warnOp  = dfl.createOperator("warn");
    Operator<Message, Message> errorOp = dfl.createOperator("error");

    inputDs.connect(splitterOp);
    splitterOp.
      connect(infoOp).
      connect(warnOp).
      connect(errorOp).
      with(new OutputSelector<Message>() {
        @Override
        public String select(Message out, String[] names) {
          return null;
        }
      });
    
    infoOp.connect(aggregateDs);
    warnOp.connect(aggregateDs);
    errorOp.connect(aggregateDs);
    
    DataflowExecutionEnvironment env = new DataflowExecutionEnvironment();
    DataflowDescriptor testDflConfig = dfl.buildDataflowDescriptor();
    System.out.println(JSONSerializer.INSTANCE.toString(testDflConfig));

    env.submit(dfl);
  }
}
