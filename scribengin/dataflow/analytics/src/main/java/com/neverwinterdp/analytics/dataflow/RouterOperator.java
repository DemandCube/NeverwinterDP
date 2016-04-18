package com.neverwinterdp.analytics.dataflow;

import com.neverwinterdp.analytics.web.WebEvent;
import com.neverwinterdp.message.Message;
import com.neverwinterdp.netty.http.client.ClientInfo;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperator;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperatorContext;
import com.neverwinterdp.util.JSONSerializer;

public class RouterOperator extends DataStreamOperator {
  @Override
  public void process(DataStreamOperatorContext ctx, Message mesg) throws Exception {
    String key = mesg.getKey();
    if(key.indexOf("odyssey") >= 0) {
      routeOdysseyEvent(ctx, mesg);
    } else  if(key.startsWith("ads-event")) {
      routeADSEvent(ctx, mesg);
    } else {
      routeWebEvent(ctx, mesg);
    }
  }
  
  void routeOdysseyEvent(DataStreamOperatorContext ctx, Message mesg) throws Exception {
    ctx.write("router-to-odyssey.event", mesg);
  }
  
  void routeADSEvent(DataStreamOperatorContext ctx, Message mesg) throws Exception {
    ctx.write("router-to-ads.statistic", mesg);
  }
  
  void routeWebEvent(DataStreamOperatorContext ctx, Message mesg) throws Exception {
    WebEvent webEvent = JSONSerializer.INSTANCE.fromBytes(mesg.getData(), WebEvent.class) ;
    ClientInfo bInfo = webEvent.getClientInfo();
    if(bInfo.navigator.appName.equals("Crawler")) {
      ctx.write("router-to-web.junk", mesg);
    } else {
      ctx.write("router-to-web.statistic", mesg);
    }
  }
}