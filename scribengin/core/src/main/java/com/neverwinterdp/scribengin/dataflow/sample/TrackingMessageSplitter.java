package com.neverwinterdp.scribengin.dataflow.sample;

import com.neverwinterdp.scribengin.dataflow.operator.Operator;
import com.neverwinterdp.scribengin.dataflow.operator.OperatorContext;
import com.neverwinterdp.scribengin.dataflow.tool.tracking.TrackingMessage;
import com.neverwinterdp.storage.Record;
import com.neverwinterdp.util.JSONSerializer;

public class TrackingMessageSplitter extends Operator {
  int count = 0;
  
  @Override
  public void process(OperatorContext ctx, Record record) throws Exception {
    TrackingMessage tMessage = JSONSerializer.INSTANCE.fromBytes(record.getData(), TrackingMessage.class) ;
    int remain = tMessage.getTrackId() % 3;
    tMessage.setStartDeliveryTime(System.currentTimeMillis());
    record.setData(JSONSerializer.INSTANCE.toBytes(tMessage));
    if(remain == 0) {
      ctx.write("error", record);
    } else if(remain == 1) {
      ctx.write("warn", record);
    } else {
      ctx.write("info", record);
    }
    count++ ;
    if(count > 0 && count % 10000 == 0) {
      ctx.commit();
    }
  }
}