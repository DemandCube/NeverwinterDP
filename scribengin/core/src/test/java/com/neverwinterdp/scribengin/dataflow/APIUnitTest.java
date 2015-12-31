package com.neverwinterdp.scribengin.dataflow;

import org.junit.Test;

import com.neverwinterdp.scribengin.dataflow.Dataflow;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.Operator;
import com.neverwinterdp.scribengin.dataflow.tracking.TrackingMessagePersister;
import com.neverwinterdp.scribengin.dataflow.tracking.TrackingMessageSplitter;
import com.neverwinterdp.storage.kafka.KafkaStorageConfig;
import com.neverwinterdp.util.JSONSerializer;

public class APIUnitTest {
  
  @Test
  public void testApi() throws Exception {
    Dataflow<Message, Message> dfl = new Dataflow<Message, Message>("dataflow");
    dfl.useWireDataSetFactory(new KafkaWireDataSetFactory("127.0.0.1:2181"));
    KafkaDataSet<Message> inputDs = 
        dfl.createInput(new KafkaStorageConfig("input", "127.0.0.1:2181", "input"));
    KafkaDataSet<Message> aggregateDs = 
        dfl.createOutput(new KafkaStorageConfig("aggregate", "127.0.0.1:2181", "aggregate"));
   
    Operator<Message, Message> splitterOp = dfl.createOperator("splitter", TrackingMessageSplitter.class);
    Operator<Message, Message> infoOp     = dfl.createOperator("info",     TrackingMessagePersister.class);
    Operator<Message, Message> warnOp     = dfl.createOperator("warn",     TrackingMessagePersister.class);
    Operator<Message, Message> errorOp    = dfl.createOperator("error",    TrackingMessagePersister.class);

    inputDs.
      connect(splitterOp);
    splitterOp.
      connect(infoOp).
      connect(warnOp).
      connect(errorOp);
    
    infoOp.connect(aggregateDs);
    warnOp.connect(aggregateDs);
    errorOp.connect(aggregateDs);
    
    DataflowDescriptor testDflConfig = dfl.buildDataflowDescriptor();
    System.out.println(JSONSerializer.INSTANCE.toString(testDflConfig));
  }
  
  static public class Message {
    private int    id;
    private String message;
    
    public Message() {}
    
    public Message(int id, String message) {
      this.id      = id;
      this.message = message;
    }
    
    public int getId() { return id; }
    public void setId(int id) { this.id = id;}
    
    public String getMessage() { return message;}
    public void setMessage(String message) { this.message = message; }
  }
}
