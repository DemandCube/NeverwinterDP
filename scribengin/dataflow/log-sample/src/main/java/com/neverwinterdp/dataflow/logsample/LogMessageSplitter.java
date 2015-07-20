package com.neverwinterdp.dataflow.logsample;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskContext;
import com.neverwinterdp.scribengin.scribe.ScribeAbstract;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.util.log.Log4jRecord;

public class LogMessageSplitter extends ScribeAbstract {
  int count = 0;
  
  public LogMessageSplitter() {
  }
  
  public void process(Record record, DataflowTaskContext ctx) throws Exception {
    Log4jRecord log4jRec = JSONSerializer.INSTANCE.fromBytes(record.getData(), Log4jRecord.class) ;
    if(log4jRec.getLoggerName().indexOf("LogSample") >= 0) {
      //Extract the log message that generate by the tool
      String level = log4jRec.getLevel().toLowerCase();
      ctx.write(level, record);
    } else {
      ctx.append(record);
    }
    count++ ;
    if(count > 0 && count % 10000 == 0) {
      ctx.commit();
    }
  }
}