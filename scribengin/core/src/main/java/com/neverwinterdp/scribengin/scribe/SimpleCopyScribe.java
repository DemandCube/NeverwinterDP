package com.neverwinterdp.scribengin.scribe;

import com.neverwinterdp.scribengin.dataflow.DataflowTaskContext;
import com.neverwinterdp.scribengin.dataflow.DataflowMessage;

public class SimpleCopyScribe extends ScribeAbstract {
  private int count = 0;
  
  public SimpleCopyScribe(){
    this.setState(ScribeState.INIT);
  }
  
  public void process(DataflowMessage dataflowMessage, DataflowTaskContext ctx) throws Exception {ctx.append(dataflowMessage);
    ctx.append(dataflowMessage);
    count++ ;
    if(count == 100) {
      setState(ScribeState.COMMITTING);
      ctx.commit();
      count = 0;
    } else{
      setState(ScribeState.BUFFERING);
    }
  }
}
