package com.neverwinterdp.analytics.dataflow;

import com.neverwinterdp.analytics.web.BrowserInfo;
import com.neverwinterdp.analytics.web.WebEvent;
import com.neverwinterdp.message.Message;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperator;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperatorContext;
import com.neverwinterdp.util.JSONSerializer;

public class RouterOperator extends DataStreamOperator {
  @Override
  public void process(DataStreamOperatorContext ctx, Message mesg) throws Exception {
    String key = mesg.getKey();
    if(key.indexOf("odyssey") >= 0) {
      routeOdysseyEvent(ctx, mesg);
    } else {
      routeWebEvent(ctx, mesg);
    }
  }
  
  void routeOdysseyEvent(DataStreamOperatorContext ctx, Message mesg) throws Exception {
    ctx.write("router-to-odyssey.statistic", mesg);
  }
  
  void routeWebEvent(DataStreamOperatorContext ctx, Message mesg) throws Exception {
    WebEvent webEvent = JSONSerializer.INSTANCE.fromBytes(mesg.getData(), WebEvent.class) ;
    BrowserInfo bInfo = webEvent.getBrowserInfo();
    if(bInfo.getBrowserFamily().equals("Crawler")) {
      ctx.write("router-to-web.junk", mesg);
    } else {
      ctx.write("router-to-web.statistic", mesg);
    }
  }
}