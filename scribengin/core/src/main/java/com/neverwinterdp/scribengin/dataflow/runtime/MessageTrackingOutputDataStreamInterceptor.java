package com.neverwinterdp.scribengin.dataflow.runtime;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.message.MessageTracking;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperatorContext;
import com.neverwinterdp.scribengin.dataflow.DataStreamSinkInterceptor;

public class MessageTrackingOutputDataStreamInterceptor extends DataStreamSinkInterceptor {
  
  @Override
  public void onInit(DataStreamOperatorContext ctx) throws Exception {
  }
  
  @Override
  public void onWrite(DataStreamOperatorContext ctx, Message message) throws Exception {
    MessageTracking mTracking = message.getMessageTracking();
  }
}
