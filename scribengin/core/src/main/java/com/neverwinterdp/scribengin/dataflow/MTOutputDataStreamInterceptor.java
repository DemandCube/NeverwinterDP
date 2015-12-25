package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.message.MessageTracking;
import com.neverwinterdp.message.MessageTrackingLog;

public class MTOutputDataStreamInterceptor extends DataStreamSinkInterceptor {
  private MTService mtService ;
  
  @Override
  public void onInit(DataStreamOperatorContext ctx) throws Exception {
    mtService = ctx.getService(MTService.class);
  }
  
  @Override
  public void onWrite(DataStreamOperatorContext ctx, Message message) throws Exception {
    MessageTracking messageTracking = message.getMessageTracking();
    String[] tag = { 
      "vm:" + ctx.getVM().getId(), "executor:" + ctx.getTaskExecutor().getId()
    };
    messageTracking.add(new MessageTrackingLog("output", tag));
    mtService.log(messageTracking);
  }
  
  @Override
  public void onCompleteCommit(DataStreamOperatorContext ctx) throws Exception {
    mtService.flush();
  }
}
