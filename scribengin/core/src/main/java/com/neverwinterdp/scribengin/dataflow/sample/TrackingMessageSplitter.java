package com.neverwinterdp.scribengin.dataflow.sample;

import com.neverwinterdp.scribengin.dataflow.api.DataStreamOperator;
import com.neverwinterdp.scribengin.dataflow.api.DataStreamOperatorContext;
import com.neverwinterdp.scribengin.dataflow.tool.tracking.TrackingMessage;
import com.neverwinterdp.storage.Record;
import com.neverwinterdp.util.JSONSerializer;

public class TrackingMessageSplitter extends DataStreamOperator {
  int count = 0;
  
  @Override
  public void process(DataStreamOperatorContext ctx, Record record) throws Exception {
    TrackingMessage tMessage = JSONSerializer.INSTANCE.fromBytes(record.getData(), TrackingMessage.class) ;
    int remain = tMessage.getTrackId() % 3;
    tMessage.setStartDeliveryTime(System.currentTimeMillis());
    record.setData(JSONSerializer.INSTANCE.toBytes(tMessage));
    if(remain == 0) {
      ctx.write("splitter-to-error", record);
    } else if(remain == 1) {
      ctx.write("splitter-to-warn", record);
    } else {
      ctx.write("splitter-to-info", record);
    }
    count++ ;
    if(count > 0 && count % 10000 == 0) {
      ctx.commit();
    }
  }
}