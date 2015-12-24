package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.message.MessageTracking;
import com.neverwinterdp.message.MessageTrackingLog;

public class MTInputDataStreamInterceptor extends DataStreamSourceInterceptor {
  private MTService mtService ;
  
  @Override
  public void onInit(DataStreamOperatorContext ctx) throws Exception {
    mtService = ctx.getService(MTService.class);
  }

  @Override
  public void onRead(DataStreamOperatorContext ctx, Message message) throws Exception {
    MessageTracking mTracking = mtService.nextMessageTracking();
    message.setMessageTracking(mTracking);
    String[] tag = { 
      "vm:" + ctx.getVM().getId(), "executor:" + ctx.getTaskExecutor().getId()
    };
    mTracking.add(new MessageTrackingLog("input", tag));
  }
}
