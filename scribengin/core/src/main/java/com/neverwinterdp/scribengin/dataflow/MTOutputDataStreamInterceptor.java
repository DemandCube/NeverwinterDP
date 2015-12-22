package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.message.MessageTracking;

public class MTOutputDataStreamInterceptor extends DataStreamSinkInterceptor {
  private MTService mtService ;
  
  @Override
  public void onInit(DataStreamOperatorContext ctx) throws Exception {
    mtService = ctx.getService(MTService.class);
  }
  
  @Override
  public void onWrite(DataStreamOperatorContext ctx, Message message) throws Exception {
    MessageTracking messageTracking = message.getMessageTracking();
    mtService.log(messageTracking);
    message.setMessageTracking(null);
  }
}
