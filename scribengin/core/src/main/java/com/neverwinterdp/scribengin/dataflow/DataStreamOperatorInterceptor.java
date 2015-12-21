package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.message.Message;

public interface DataStreamOperatorInterceptor {

  public void preProcess(DataStreamOperatorContext ctx, Message message) throws Exception;
  
  public void postProcess(DataStreamOperatorContext ctx, Message message) throws Exception;

}
