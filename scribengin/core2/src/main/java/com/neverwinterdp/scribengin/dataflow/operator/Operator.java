package com.neverwinterdp.scribengin.dataflow.operator;

import com.neverwinterdp.scribengin.storage.Record;

public abstract class Operator {
  abstract public void process(OperatorContext ctx, Record record) throws Exception;
}