package com.neverwinterdp.scribengin.dataflow.sample;

import com.neverwinterdp.scribengin.dataflow.DataflowMessage;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskContext;
import com.neverwinterdp.scribengin.dataflow.tool.tracking.TrackingMessage;
import com.neverwinterdp.scribengin.scribe.ScribeAbstract;
import com.neverwinterdp.util.JSONSerializer;

public class TrackingMessageSplitter extends ScribeAbstract {
  int count = 0;
  
  public void process(DataflowMessage dflMessage, DataflowTaskContext ctx) throws Exception {
    TrackingMessage tMessage = JSONSerializer.INSTANCE.fromBytes(dflMessage.getData(), TrackingMessage.class) ;
    int remain = tMessage.getTrackId() % 3;
    if(remain == 0) {
      ctx.write("error", dflMessage);
    } else if(remain == 1) {
      ctx.write("warn", dflMessage);
    } else {
      ctx.write("info", dflMessage);
    }
    count++ ;
    if(count > 0 && count % 10000 == 0) {
      ctx.commit();
    }
  }
}