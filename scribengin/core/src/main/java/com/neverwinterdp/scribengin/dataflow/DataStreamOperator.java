package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.message.Message;

public abstract class DataStreamOperator {
  abstract public void process(DataStreamOperatorContext ctx, Message record) throws Exception;
}