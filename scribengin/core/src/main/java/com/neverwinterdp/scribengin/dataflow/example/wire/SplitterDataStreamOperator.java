package com.neverwinterdp.scribengin.dataflow.example.wire;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperator;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperatorContext;

public class SplitterDataStreamOperator extends DataStreamOperator{
  @Override
  public void process(DataStreamOperatorContext ctx, Message record) throws Exception {
    int oddOrEven = (new String(record.getData()).length()) % 2;
    if(oddOrEven == 1) { //Odd length
      ctx.write("splitteroperator-to-oddoperator", record);
    } else { //Even length
      ctx.write("splitteroperator-to-evenoperator", record);
    }
  }
}
