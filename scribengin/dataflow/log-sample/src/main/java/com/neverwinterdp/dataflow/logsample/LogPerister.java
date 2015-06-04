package com.neverwinterdp.dataflow.logsample;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskContext;
import com.neverwinterdp.scribengin.scribe.ScribeAbstract;

public class LogPerister extends ScribeAbstract {
  public void process(Record record, DataflowTaskContext ctx) throws Exception {
    String[] sink = ctx.getAvailableSinks();
    for(String selSink : sink) {
      ctx.write(selSink, record);
    }
  }
}