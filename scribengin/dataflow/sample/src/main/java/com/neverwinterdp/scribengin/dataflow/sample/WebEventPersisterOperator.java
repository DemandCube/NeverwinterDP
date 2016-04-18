package com.neverwinterdp.scribengin.dataflow.sample;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperator;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperatorContext;
import com.neverwinterdp.util.JSONSerializer;

public class WebEventPersisterOperator extends DataStreamOperator {
  
  @Override
  public void process(DataStreamOperatorContext ctx, Message mesg) throws Exception {
    //Convert the message data to WebEvent object
    WebEvent webEvent = JSONSerializer.INSTANCE.fromBytes(mesg.getData(), WebEvent.class) ;
    //Do some data transformation to the web event object
    
    //Convert back the web event to bytes
    mesg.setData(JSONSerializer.INSTANCE.toBytes(webEvent));
    ctx.write(mesg);
  }
  
}