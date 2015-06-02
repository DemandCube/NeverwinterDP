package com.neverwinterdp.dataflow.logsample;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskContext;
import com.neverwinterdp.scribengin.scribe.ScribeAbstract;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.util.log.Log4jRecord;

public class LogSampleDataProcessor extends ScribeAbstract {
  public void process(Record record, DataflowTaskContext ctx) throws Exception {
    Log4jRecord log4jRec = JSONSerializer.INSTANCE.fromBytes(record.getData(), Log4jRecord.class) ;
    ctx.write(log4jRec.getLevel().toLowerCase(), record);
    System.out.println("Record: " + log4jRec.getId() + ": " + log4jRec.getMessage());
  }
}