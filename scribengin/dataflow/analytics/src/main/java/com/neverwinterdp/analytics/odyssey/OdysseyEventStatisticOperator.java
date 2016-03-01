package com.neverwinterdp.analytics.odyssey;

import java.util.Calendar;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperator;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperatorContext;
import com.neverwinterdp.util.JSONSerializer;

public class OdysseyEventStatisticOperator extends DataStreamOperator {
  public void onInit(DataStreamOperatorContext ctx) throws Exception {
  }
  
  public void onPostCommit(DataStreamOperatorContext ctx) throws Exception {
  }
  
  @Override
  public void process(DataStreamOperatorContext ctx, Message record) throws Exception {
    Event event = JSONSerializer.INSTANCE.fromBytes(record.getData(), Event.class) ;

    Calendar cal = Calendar.getInstance();
    cal.setTimeInMillis(event.getTimestamp().getTime());
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    
    ctx.write(record);
  }
  
  
}