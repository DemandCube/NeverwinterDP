package com.neverwinterdp.analytics.web;

import java.security.MessageDigest;
import java.util.Date;

import javax.xml.bind.annotation.adapters.HexBinaryAdapter;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperator;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperatorContext;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.util.UrlParser;

public class WebEventOperator extends DataStreamOperator {
  private MessageDigest md5Digest ;
  private HexBinaryAdapter hexBinaryAdapter = new HexBinaryAdapter();
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
    wVisit.setVisitorId(webEvent.getClientInfo().user.visitorId);
    wVisit.setClientIpAddress(webEvent.getClientInfo().user.ipAddress);
    byte[] bytes = md5Digest.digest((urlParser.getUrl() + wVisit.getVisitorId()).getBytes());
    String visitId = hexBinaryAdapter.marshal(bytes);

    wVisit.setVisitId(visitId);
    
    wVisit.setSpentTime(webEvent.getClientInfo().user.spentTime);
    mesg.setData(JSONSerializer.INSTANCE.toBytes(wVisit));
    ctx.write(mesg);
  }
}