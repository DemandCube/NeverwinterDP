package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.message.MessageTracking;

public class MTOutputDataStreamInterceptor extends DataStreamSinkInterceptor {
  
  @Override
  public void onInit(DataStreamOperatorContext ctx) throws Exception {
  }
  
  @Override
  public void onWrite(DataStreamOperatorContext ctx, Message message) throws Exception {
    MessageTracking mTracking = message.getMessageTracking();
    message.setMessageTracking(null);
  }
}
