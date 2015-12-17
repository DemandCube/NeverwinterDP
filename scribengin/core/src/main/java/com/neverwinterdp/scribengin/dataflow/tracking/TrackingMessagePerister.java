package com.neverwinterdp.scribengin.dataflow.tracking;

import java.util.Set;

import com.neverwinterdp.scribengin.dataflow.DataStreamOperator;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperatorContext;
import com.neverwinterdp.storage.Record;
import com.neverwinterdp.util.JSONSerializer;

public class TrackingMessagePerister extends DataStreamOperator {
  int count = 0 ;
  
  @Override
  public void process(DataStreamOperatorContext ctx, Record record) throws Exception {
    TrackingMessage tMessage = JSONSerializer.INSTANCE.fromBytes(record.getData(), TrackingMessage.class) ;
    tMessage.setEndDeliveryTime(System.currentTimeMillis());
    record.setData(JSONSerializer.INSTANCE.toBytes(tMessage));
    Set<String> sink = ctx.getAvailableOutputs();
    for(String selSink : sink) {
      ctx.write(selSink, record);
    }
    
    count++;
    if(count > 0 && count % 10000 == 0) {
      ctx.commit();
    }
  }
}