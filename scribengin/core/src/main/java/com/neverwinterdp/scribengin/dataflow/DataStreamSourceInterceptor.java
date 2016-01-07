package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.message.Message;

abstract public class DataStreamSourceInterceptor {
  public void onInit(DataStreamOperatorContext ctx) throws Exception {
  }
  
  public void onRead(DataStreamOperatorContext ctx, Message message) throws Exception {
  }
  
  public void onPrepareCommit(DataStreamOperatorContext ctx) throws Exception {
  }
  
  public void onCompleteCommit(DataStreamOperatorContext ctx) throws Exception {
  }
  
  static public DataStreamSourceInterceptor[] load(DataStreamOperatorContext ctx, String[] type) throws Exception {
    if(type == null || type.length == 0) {
      return new DataStreamSourceInterceptor[0];
    }
    DataStreamSourceInterceptor[] interceptor = new DataStreamSourceInterceptor[type.length];
    for(int i = 0; i < interceptor.length; i++) {
      Class<? extends DataStreamSourceInterceptor> clazz = 
          (Class<? extends DataStreamSourceInterceptor>) Class.forName(type[i]);
      interceptor[i] = clazz.newInstance();
      interceptor[i].onInit(ctx);
    }
    return interceptor;
  }
}
