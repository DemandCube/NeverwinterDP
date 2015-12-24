package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.message.MessageTracking;
import com.neverwinterdp.message.MessageTrackingLog;
import com.neverwinterdp.scribengin.dataflow.runtime.DataStreamOperatorDescriptor;

public class MTDataStreamOperatorInterceptor implements DataStreamOperatorInterceptor {
  
  @Override
  public void preProcess(DataStreamOperatorContext ctx, Message message) throws Exception {
    DataStreamOperatorDescriptor descriptor = ctx.getDescriptor();
    onMessage(descriptor.getOperator() + ":preProcess", ctx, message);
  }

  @Override
  public void postProcess(DataStreamOperatorContext ctx, Message message) throws Exception {
    DataStreamOperatorDescriptor descriptor = ctx.getDescriptor();
    onMessage(descriptor.getOperator() + ":postProcess", ctx, message);
  }
  
  void onMessage(String logName, DataStreamOperatorContext ctx, Message message) {
    MessageTracking mTracking = message.getMessageTracking();
    String[] tag = { 
      "vm:" + ctx.getVM().getId(), "executor:" + ctx.getTaskExecutor().getId()
    };
    mTracking.add(new MessageTrackingLog(logName, tag));
  }
}
