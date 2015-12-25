package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.message.Message;

abstract public class DataStreamSinkInterceptor {
  public void onInit(DataStreamOperatorContext ctx) throws Exception {
  }
  
  public void onWrite(DataStreamOperatorContext ctx, Message message) throws Exception {
  }
  
  public void onPrepareCommit(DataStreamOperatorContext ctx) throws Exception {
  }
  
  public void onCompleteCommit(DataStreamOperatorContext ctx) throws Exception {
  }
  
  static public DataStreamSinkInterceptor[] load(DataStreamOperatorContext ctx, String[] type) throws Exception {
    if(type == null || type.length == 0) {
      return new DataStreamSinkInterceptor[0];
    }
    DataStreamSinkInterceptor[] interceptor = new DataStreamSinkInterceptor[type.length];
    for(int i = 0; i < interceptor.length; i++) {
      Class<? extends DataStreamSinkInterceptor> clazz = 
          (Class<? extends DataStreamSinkInterceptor>) Class.forName(type[i]);
      interceptor[i] = clazz.newInstance();
      interceptor[i].onInit(ctx);
    }
    return interceptor;
  }
}
