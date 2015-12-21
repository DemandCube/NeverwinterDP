package com.neverwinterdp.scribengin.dataflow.tracking;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperator;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperatorContext;
import com.neverwinterdp.util.JSONSerializer;

public class TrackingMessageSplitter extends DataStreamOperator {
  int count = 0;
  
  @Override
  public void process(DataStreamOperatorContext ctx, Message record) throws Exception {
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