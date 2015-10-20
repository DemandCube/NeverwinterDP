package com.neverwinterdp.scribengin.dataflow.sample;

import java.util.Set;

import com.neverwinterdp.scribengin.dataflow.operator.Operator;
import com.neverwinterdp.scribengin.dataflow.operator.OperatorContext;
import com.neverwinterdp.scribengin.storage.Record;

public class TrackingMessagePerister extends Operator {
  int count = 0 ;
  
  @Override
  public void process(OperatorContext ctx, Record record) throws Exception {
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