package com.neverwinterdp.dataflow.logsample;

import com.neverwinterdp.scribengin.dataflow.DataflowMessage;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskContext;
import com.neverwinterdp.scribengin.scribe.ScribeAbstract;

public class LogMessagePerister extends ScribeAbstract {
  int count = 0 ;
  
  public void process(DataflowMessage dflMessage, DataflowTaskContext ctx) throws Exception {
    String[] sink = ctx.getAvailableSinks();
    for(String selSink : sink) {
      ctx.write(selSink, dflMessage);
    }
    
    count++ ;
    if(count > 0 && count % 10000 == 0) {
      ctx.commit();
    }
  }
}