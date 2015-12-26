package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.message.Message;

abstract public class DataStreamOperatorInterceptor {

  public void onInit(DataStreamOperatorContext ctx) throws Exception {
  }
  
  public void preProcess(DataStreamOperatorContext ctx, Message message) throws Exception {
  }
  
  public void postProcess(DataStreamOperatorContext ctx, Message message) throws Exception {
  }

  static public DataStreamOperatorInterceptor[] load(DataStreamOperatorContext ctx, String[] type) throws Exception {
    if(type == null || type.length == 0) {
      return new DataStreamOperatorInterceptor[0];
    }
    DataStreamOperatorInterceptor[] interceptor = new DataStreamOperatorInterceptor[type.length];
    for(int i = 0; i < interceptor.length; i++) {
      Class<? extends DataStreamOperatorInterceptor> clazz = 
          (Class<? extends DataStreamOperatorInterceptor>) Class.forName(type[i]);
      interceptor[i] = clazz.newInstance();
      interceptor[i].onInit(ctx);
    }
    return interceptor;
  }
}