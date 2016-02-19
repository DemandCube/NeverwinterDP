package com.neverwinterdp.analytics.dataflow;

import java.util.Calendar;
import java.util.List;

import com.neverwinterdp.analytics.web.WebEvent;
import com.neverwinterdp.analytics.web.stat.WebPageStat;
import com.neverwinterdp.analytics.web.stat.WebPageStatCollector;
import com.neverwinterdp.analytics.web.stat.WebSiteStat;
import com.neverwinterdp.analytics.web.stat.WebSiteStatCollector;
import com.neverwinterdp.es.ESClient;
import com.neverwinterdp.es.ESObjectClient;
import com.neverwinterdp.message.Message;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperator;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperatorContext;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperatorDescriptor;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.util.UrlParser;

public class StatisticOperator extends DataStreamOperator {
  private String[]           connect = { "elasticsearch-1" };
  private ESObjectClient<WebPageStat> esWebPageStatClient;
  private ESObjectClient<WebSiteStat> esWebSiteStatClient;
  
  private WebPageStatCollector webPageCollector = new WebPageStatCollector();
  private WebSiteStatCollector webSiteCollector = new WebSiteStatCollector();
  
  public void onInit(DataStreamOperatorContext ctx) throws Exception {
    DataStreamOperatorDescriptor descriptor = ctx.getDescriptor() ;
    ESClient esClient = new ESClient(connect);
    synchronized(getClass()) {
      esWebPageStatClient = new ESObjectClient<WebPageStat>(esClient, "webpage-stat", WebPageStat.class) ;
      esWebPageStatClient.getESClient().waitForConnected(24 * 60 * 60 * 1000) ;
      if(!esWebPageStatClient.isCreated()) {
        esWebPageStatClient.createIndex();
      }
      
      esWebSiteStatClient = new ESObjectClient<WebSiteStat>(esClient, "website-stat", WebSiteStat.class) ;
      esWebSiteStatClient.getESClient().waitForConnected(24 * 60 * 60 * 1000) ;
      if(!esWebSiteStatClient.isCreated()) {
        esWebSiteStatClient.createIndex();
      }
    }
  }
  
  public void onPostCommit(DataStreamOperatorContext ctx) throws Exception {
    List<WebPageStat> webPageStats = webPageCollector.takeWebPageStat();
    for(int i = 0; i < webPageStats.size(); i++) {
      WebPageStat wpStat = webPageStats.get(i);
      esWebPageStatClient.put(wpStat, wpStat.uniqueId());
    }
    
    List<WebSiteStat> webSiteStats = webSiteCollector.takeWebSiteStat();
    for(int i = 0; i < webSiteStats.size(); i++) {
      WebSiteStat wsStat = webSiteStats.get(i);
      esWebSiteStatClient.put(wsStat, wsStat.uniqueId());
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
    UrlParser urlParser = new UrlParser(webEvent.getUrl());
    webPageCollector.log(periodTimestamp, urlParser, webEvent);
    webSiteCollector.log(periodTimestamp, urlParser, webEvent);
    
    ctx.write(record);
  }
  
  
}