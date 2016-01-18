package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.message.MessageTrackingLog;
import com.neverwinterdp.scribengin.dataflow.runtime.DataStreamOperatorDescriptor;

public class MTDataStreamOperatorInterceptor extends DataStreamOperatorInterceptor {
  
  @Override
  public void preProcess(DataStreamOperatorContext ctx, Message message) throws Exception {
    DataStreamOperatorDescriptor descriptor = ctx.getDescriptor();
    onMessage(descriptor.getOperatorName() + ":preProcess", ctx, message);
  }

  @Override
  public void postProcess(DataStreamOperatorContext ctx, Message message) throws Exception {
    DataStreamOperatorDescriptor descriptor = ctx.getDescriptor();
    onMessage(descriptor.getOperatorName() + ":postProcess", ctx, message);
  }
  
  void onMessage(String logName, DataStreamOperatorContext ctx, Message message) {
    String[] tag = { 
      "vm:" + ctx.getVM().getVmId(), "executor:" + ctx.getTaskExecutor().getId()
    };
    message.getMessageTracking().add(new MessageTrackingLog(logName, tag));
  }
}
