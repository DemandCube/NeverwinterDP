package com.neverwinterdp.wa.dataflow;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperator;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperatorContext;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.wa.event.BrowserInfo;
import com.neverwinterdp.wa.event.WebEvent;

public class RouterOperator extends DataStreamOperator {
  @Override
  public void process(DataStreamOperatorContext ctx, Message mesg) throws Exception {
    WebEvent webEvent = JSONSerializer.INSTANCE.fromBytes(mesg.getData(), WebEvent.class) ;
    BrowserInfo bInfo = webEvent.getBrowserInfo();
    if(bInfo.getBrowserFamily().equals("Crawler")) {
      ctx.write("router-to-junk-stats", mesg);
    } else {
      ctx.write("router-to-hit-stats", mesg);
    }
  }
  
}