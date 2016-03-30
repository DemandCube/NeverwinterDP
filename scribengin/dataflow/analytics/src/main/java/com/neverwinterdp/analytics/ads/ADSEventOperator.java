package com.neverwinterdp.analytics.ads;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperator;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperatorContext;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.util.UrlParser;

public class ADSEventOperator extends DataStreamOperator {
  @Override
  public void process(DataStreamOperatorContext ctx, Message record) throws Exception {
    ADSEvent event = JSONSerializer.INSTANCE.fromBytes(record.getData(), ADSEvent.class) ;
    UrlParser urlParser = new UrlParser(event.getWebpageUrl());
    event.setHost(urlParser.getHost());
    event.setWebpageUrl(urlParser.getUrl());
    String visitId = event.getWebpageUrl() + "#" + event.getAdUrl() + "#" + event.getVisitorId();
    event.setVisitId(visitId);
    record.setData(JSONSerializer.INSTANCE.toBytes(event));
    ctx.write(record);
  }
}