package com.neverwinterdp.analytics.web;

import java.util.Calendar;
import java.util.List;

import com.neverwinterdp.analytics.web.stat.WebPageStat;
import com.neverwinterdp.analytics.web.stat.WebPageStatCollector;
import com.neverwinterdp.analytics.web.stat.VisitorStat;
import com.neverwinterdp.analytics.web.stat.VisitorStatCollector;
import com.neverwinterdp.es.ESClient;
import com.neverwinterdp.es.ESObjectClient;
import com.neverwinterdp.message.Message;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperator;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperatorContext;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.util.UrlParser;

public class WebEventStatisticOperator extends DataStreamOperator {
  private String[]           connect = { "elasticsearch-1" };
  private ESObjectClient<WebPageStat> esWebPageStatClient;
  private ESObjectClient<VisitorStat> esVisitorStatClient;
  
  private WebPageStatCollector webPageCollector     = new WebPageStatCollector();
  private VisitorStatCollector visitorStatCollector = new VisitorStatCollector();
  
  public void onInit(DataStreamOperatorContext ctx) throws Exception {
    ESClient esClient = new ESClient(connect);
    synchronized(getClass()) {
      esWebPageStatClient = new ESObjectClient<WebPageStat>(esClient, "webpage-stat", WebPageStat.class) ;
      esWebPageStatClient.getESClient().waitForConnected(24 * 60 * 60 * 1000) ;
      if(!esWebPageStatClient.isCreated()) {
        esWebPageStatClient.createIndex();
      }
      
      esVisitorStatClient = new ESObjectClient<VisitorStat>(esClient, "visitor-stat", VisitorStat.class) ;
      esWebPageStatClient.getESClient().waitForConnected(24 * 60 * 60 * 1000) ;
      if(!esWebPageStatClient.isCreated()) {
        esWebPageStatClient.createIndex();
      }
    }
  }
  
  public void onPostCommit(DataStreamOperatorContext ctx) throws Exception {
    List<WebPageStat> webPageStats = webPageCollector.takeWebPageStat();
    for(int i = 0; i < webPageStats.size(); i++) {
      WebPageStat wpStat = webPageStats.get(i);
      esWebPageStatClient.put(wpStat, wpStat.uniqueId());
    }
    
    List<VisitorStat> visitorStats = visitorStatCollector.takeVisitorStats();
    for(int i = 0; i < visitorStats.size(); i++) {
      VisitorStat visitorStat = visitorStats.get(i);
      esVisitorStatClient.put(visitorStat, visitorStat.uniqueId());
    }
  }
  
  @Override
  public void process(DataStreamOperatorContext ctx, Message record) throws Exception {
    WebEvent webEvent = JSONSerializer.INSTANCE.fromBytes(record.getData(), WebEvent.class) ;

    Calendar cal = Calendar.getInstance();
    cal.setTimeInMillis(webEvent.getTimestamp());
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    
    long periodTimestamp = cal.getTimeInMillis();
    UrlParser urlParser = new UrlParser(webEvent.getClientInfo().webpage.url);
    //System.err.println("host = " + urlParser.getHost() + ", page = " + urlParser.getPath());
    webPageCollector.log(periodTimestamp, urlParser, webEvent);
    visitorStatCollector.log(periodTimestamp, urlParser.getHost(), webEvent);
    ctx.write(record);
  }
  
  
}