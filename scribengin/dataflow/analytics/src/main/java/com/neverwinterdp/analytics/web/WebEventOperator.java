package com.neverwinterdp.analytics.web;

import java.security.MessageDigest;
import java.util.Date;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperator;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperatorContext;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.util.UrlParser;

public class WebEventOperator extends DataStreamOperator {
  private MessageDigest md5Digest ;
  
  public void onInit(DataStreamOperatorContext ctx) throws Exception {
    md5Digest = MessageDigest.getInstance("MD5");
  }
  
  public void onPostCommit(DataStreamOperatorContext ctx) throws Exception {
  }
  
  @Override
  public void process(DataStreamOperatorContext ctx, Message mesg) throws Exception {
    WebEvent webEvent = JSONSerializer.INSTANCE.fromBytes(mesg.getData(), WebEvent.class) ;
    UrlParser urlParser = new UrlParser(webEvent.getClientInfo().webpage.url);
    
    WebpageVisit wVisit = new WebpageVisit();
    wVisit.setEventId(webEvent.getEventId());
    wVisit.setTimestamp(new Date(webEvent.getTimestamp()));
    wVisit.setHost(urlParser.getHost());
    wVisit.setPath(urlParser.getPath());
    wVisit.setClientIpAddress(webEvent.getClientInfo().user.visitorId);
    wVisit.setVisitId(new String(md5Digest.digest((urlParser.getUrl() + wVisit.getVisitorId()).getBytes())));
    wVisit.setSpentTime(webEvent.getClientInfo().user.spentTime);
    mesg.setData(JSONSerializer.INSTANCE.toBytes(wVisit));
    ctx.write(mesg);
  }
}