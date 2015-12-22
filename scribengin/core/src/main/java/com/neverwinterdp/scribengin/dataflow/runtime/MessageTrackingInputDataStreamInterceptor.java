package com.neverwinterdp.scribengin.dataflow.runtime;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.message.MessageTracking;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperatorContext;
import com.neverwinterdp.scribengin.dataflow.DataStreamSourceInterceptor;

public class MessageTrackingInputDataStreamInterceptor extends DataStreamSourceInterceptor {
  
  @Override
  public void onInit(DataStreamOperatorContext ctx) throws Exception {
  }
  
  @Override
  public void onRead(DataStreamOperatorContext ctx, Message message) throws Exception {
    MessageTracking mTracking = new MessageTracking();
    message.setMessageTracking(mTracking);
  }
}
