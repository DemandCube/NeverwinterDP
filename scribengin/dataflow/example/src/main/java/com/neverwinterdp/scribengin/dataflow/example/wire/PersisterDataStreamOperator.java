package com.neverwinterdp.scribengin.dataflow.example.wire;

import java.util.Set;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperator;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperatorContext;

public class PersisterDataStreamOperator extends DataStreamOperator{

  @Override
  public void process(DataStreamOperatorContext ctx, Message record) throws Exception {
    Set<String> sink = ctx.getAvailableOutputs();
    //For each sink, write the record
    for(String selSink : sink) {
      ctx.write(selSink, record);
    }
  }

}
