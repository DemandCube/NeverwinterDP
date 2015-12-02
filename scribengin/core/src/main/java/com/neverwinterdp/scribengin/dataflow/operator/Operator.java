package com.neverwinterdp.scribengin.dataflow.operator;

import com.neverwinterdp.storage.Record;

public abstract class Operator {
  abstract public void process(OperatorContext ctx, Record record) throws Exception;
}