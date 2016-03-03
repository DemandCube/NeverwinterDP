package com.neverwinterdp.analytics.ads;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperator;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperatorContext;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.util.UrlParser;

public class ADSEventStatisticOperator extends DataStreamOperator {
  @Override
  public void process(DataStreamOperatorContext ctx, Message record) throws Exception {
    ADSEvent event = JSONSerializer.INSTANCE.fromBytes(record.getData(), ADSEvent.class) ;
    UrlParser urlParser = new UrlParser(event.getWebpageUrl());
    event.setHost(urlParser.getHost());
    event.setWebpageUrl(urlParser.getUrl());
    String key = event.getWebpageUrl() + "#" + event.getAdUrl() + "#" + event.getVisitorId();
    record.setKey(key);
    record.setData(JSONSerializer.INSTANCE.toBytes(event));
    ctx.write(record);
  }
}