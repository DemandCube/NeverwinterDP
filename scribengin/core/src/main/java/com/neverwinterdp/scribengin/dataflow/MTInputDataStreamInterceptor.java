package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.message.MessageTracking;

public class MTInputDataStreamInterceptor extends DataStreamSourceInterceptor {
  
  @Override
  public void onInit(DataStreamOperatorContext ctx) throws Exception {
  }
  
  @Override
  public void onRead(DataStreamOperatorContext ctx, Message message) throws Exception {
    MessageTracking mTracking = new MessageTracking();
    message.setMessageTracking(mTracking);
  }
}
