package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.message.Message;

abstract public class DataStreamSinkInterceptor {
  public void onInit(DataStreamOperatorContext ctx) throws Exception {
  }
  
  public void onWrite(DataStreamOperatorContext ctx, Message message) throws Exception {
  }
}
