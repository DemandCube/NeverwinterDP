package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.message.Message;

public abstract class DataStreamOperator {
  public void onInit(DataStreamOperatorContext ctx) throws Exception {
  }
  
  public void onPreCommit(DataStreamOperatorContext ctx) throws Exception {
  }
  
  public void onPostCommit(DataStreamOperatorContext ctx) throws Exception {
  }
  
  abstract public void process(DataStreamOperatorContext ctx, Message record) throws Exception;
}