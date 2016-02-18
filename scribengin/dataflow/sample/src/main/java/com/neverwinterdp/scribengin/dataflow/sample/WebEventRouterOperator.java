package com.neverwinterdp.scribengin.dataflow.sample;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperator;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperatorContext;
import com.neverwinterdp.util.JSONSerializer;

public class WebEventRouterOperator extends DataStreamOperator {
  
  @Override
  public void process(DataStreamOperatorContext ctx, Message mesg) throws Exception {
    WebEvent webEvent = JSONSerializer.INSTANCE.fromBytes(mesg.getData(), WebEvent.class) ;
    if("Crawler".equals(webEvent.getAttributes().attribute("browser.family"))) {
      ctx.write("router-to-junk", mesg);
    } else {
      ctx.write("router-to-archive", mesg);
    }
  }
  
}