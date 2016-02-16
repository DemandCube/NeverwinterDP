package com.neverwinterdp.wa.dataflow;

import java.util.Random;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperator;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperatorContext;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.wa.event.WebEvent;

public class RouterOperator extends DataStreamOperator {
  private Random rand = new Random();
  @Override
  public void process(DataStreamOperatorContext ctx, Message mesg) throws Exception {
    WebEvent webEvent = JSONSerializer.INSTANCE.fromBytes(mesg.getData(), WebEvent.class) ;
    if(rand.nextInt(2) == 1) {
      ctx.write("router-to-hit-stats", mesg);
    } else {
      ctx.write("router-to-junk-stats", mesg);
    }
  }
  
}