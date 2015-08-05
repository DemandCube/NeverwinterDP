package com.neverwinterdp.dataflow.logsample;

import java.util.concurrent.atomic.AtomicInteger;

import com.neverwinterdp.scribengin.dataflow.DataflowMessage;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskContext;
import com.neverwinterdp.scribengin.scribe.ScribeAbstract;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.util.log.Log4jRecord;

public class LogMessageSplitter extends ScribeAbstract {
  static AtomicInteger allCounter = new AtomicInteger() ;
  
  int count = 0;
  
  public LogMessageSplitter() {
  }
  
  public void process(DataflowMessage dflMessage, DataflowTaskContext ctx) throws Exception {
    Log4jRecord log4jRec = JSONSerializer.INSTANCE.fromBytes(dflMessage.getData(), Log4jRecord.class) ;
    if(log4jRec.getLoggerName().indexOf("LogSample") >= 0) {
      String level = log4jRec.getLevel().toLowerCase();
      if("EOS".equals(log4jRec.getMessage())) {
        //ctx.write(level, new DataflowMessage(DataflowInstruction.END_OF_DATASTREAM));
        ctx.setComplete();
      } else {
        //Extract the log message that generate by the tool
        ctx.write(level, dflMessage);
      }
    } else {
      ctx.append(dflMessage);
    }
    count++ ;
    if(count > 0 && count % 10000 == 0) {
      ctx.commit();
    }
    //System.err.println("Splitter process " + allCounter.incrementAndGet() + ", key = " + dflMessage.getKey()) ;
  }
}