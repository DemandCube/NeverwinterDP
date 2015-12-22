package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.message.Message;

abstract public class DataStreamSourceInterceptor {
  public void onInit(DataStreamOperatorContext ctx) throws Exception {
  }
  
  public void onRead(DataStreamOperatorContext ctx, Message message) throws Exception {
  }
}
