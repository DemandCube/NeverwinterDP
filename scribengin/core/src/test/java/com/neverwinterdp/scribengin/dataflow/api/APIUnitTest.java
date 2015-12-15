package com.neverwinterdp.scribengin.dataflow.api;

import org.junit.Test;

import com.neverwinterdp.scribengin.dataflow.config.DataflowConfig;
import com.neverwinterdp.util.JSONSerializer;

public class APIUnitTest {
  
  @Test
  public void testApi() throws Exception {
    
    Dataflow<Message, Message> testDfl = new Dataflow<Message, Message>("dataflow");
    
    DataStream<Message> inputDs     = testDfl.addInput(new DataStream<Message>("input"));
    DataStream<Message> aggregateDs = testDfl.addOutput(new DataStream<Message>("aggregate"));
    
    Operator<Message, Message> splitterOp = inputDs.connect(new Operator<Message, Message>("splitter"));
    
    Operator<Message, Message> infoOp = splitterOp.outConnect(new Operator<Message, Message>("info"));
    infoOp.outConnect(aggregateDs);
    
    Operator<Message, Message> warnOp = splitterOp.outConnect(new Operator<Message, Message>("warn"));
    warnOp.outConnect(aggregateDs);
    
    Operator<Message, Message> errorOp = splitterOp.outConnect(new Operator<Message, Message>("error"));
    errorOp.outConnect(aggregateDs);
    
    DataflowExecutionEnvironment env = new DataflowExecutionEnvironment();
    DataflowConfig testDflConfig = env.build(testDfl);
    System.out.println(JSONSerializer.INSTANCE.toString(testDflConfig));
  }
}
