package com.neverwinterdp.scribengin.dataflow.sample.log;

import java.util.concurrent.atomic.AtomicInteger;

import com.neverwinterdp.scribengin.dataflow.operator.Operator;
import com.neverwinterdp.scribengin.dataflow.operator.OperatorContext;
import com.neverwinterdp.scribengin.storage.Record;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.util.log.Log4jRecord;

public class LogSplitterOperator extends Operator {
  static AtomicInteger allCounter = new AtomicInteger() ;
  
  int count = 0;
  
  @Override
  public void process(OperatorContext ctx, Record record) throws Exception {
    Log4jRecord log4jRec = JSONSerializer.INSTANCE.fromBytes(record.getData(), Log4jRecord.class) ;
    if(log4jRec.getLoggerName().indexOf("LogSample") >= 0) {
      if("EOS".equals(log4jRec.getMessage())) {
        //ctx.setComplete();
      } else {
        String level = log4jRec.getLevel().toLowerCase();
        ctx.write(level, record);
      }
    }
    count++ ;
    if(count > 0 && count % 10000 == 0) {
      ctx.commit();
    }
  }
}