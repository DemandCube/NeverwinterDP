package com.neverwinterdp.analytics.ads;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperator;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperatorContext;
import com.neverwinterdp.util.JSONSerializer;

public class ADSEventStatisticOperator extends DataStreamOperator {
  public void onInit(DataStreamOperatorContext ctx) throws Exception {
  }
  
  public void onPostCommit(DataStreamOperatorContext ctx) throws Exception {
  }
  
  @Override
  public void process(DataStreamOperatorContext ctx, Message record) throws Exception {
    ADSEvent event = JSONSerializer.INSTANCE.fromBytes(record.getData(), ADSEvent.class) ;
    ctx.write(record);
  }
}