package com.neverwinterdp.analytics.odyssey;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperator;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperatorContext;

public class OdysseyOperator extends DataStreamOperator {
  public void onInit(DataStreamOperatorContext ctx) throws Exception {
  }
  
  public void onPostCommit(DataStreamOperatorContext ctx) throws Exception {
  }
  
  @Override
  public void process(DataStreamOperatorContext ctx, Message record) throws Exception {
    String key = new String(record.getKey());
    if(key.startsWith("odyssey-action-event")) {
      //ActionEvent event = JSONSerializer.INSTANCE.fromBytes(record.getData(), ActionEvent.class) ;
      ctx.write("odyssey.action-event", record);
    } else if(key.startsWith("odyssey-mouse-move")) {
      //MouseMoveEvent event = JSONSerializer.INSTANCE.fromBytes(record.getData(), MouseMoveEvent.class) ;
      ctx.write("odyssey.mouse-move", record);
    }
  }
}