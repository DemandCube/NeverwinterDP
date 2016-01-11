package com.neverwinterdp.scribengin.dataflow.example.simple;

import java.util.Set;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperator;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperatorContext;

//You must extend DataStreamOperator
public class SimpleDataStreamOperator extends DataStreamOperator{

  //You must override the process() method
  @Override
  public void process(DataStreamOperatorContext ctx, Message record)
      throws Exception {
    
    //Get all sinks
    Set<String> sink = ctx.getAvailableOutputs();
    //For each sink, write the record
    for(String selSink : sink) {
      ctx.write(selSink, record);
    }
    //ctx.commit();
  }

}
