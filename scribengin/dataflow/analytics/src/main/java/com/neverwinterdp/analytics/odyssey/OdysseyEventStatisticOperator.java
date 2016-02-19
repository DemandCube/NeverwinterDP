package com.neverwinterdp.analytics.odyssey;

import java.util.Calendar;

import com.neverwinterdp.analytics.web.stat.WebPageStat;
import com.neverwinterdp.analytics.web.stat.WebSiteStat;
import com.neverwinterdp.es.ESClient;
import com.neverwinterdp.es.ESObjectClient;
import com.neverwinterdp.message.Message;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperator;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperatorContext;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperatorDescriptor;
import com.neverwinterdp.util.JSONSerializer;

public class OdysseyEventStatisticOperator extends DataStreamOperator {
  private String[]  esAddresses = { "elasticsearch-1:9300" };
  private ESObjectClient<WebPageStat> esWebPageStatClient;
  private ESObjectClient<WebSiteStat> esWebSiteStatClient;
  
  public void onInit(DataStreamOperatorContext ctx) throws Exception {
    DataStreamOperatorDescriptor descriptor = ctx.getDescriptor() ;
    ESClient esClient = new ESClient(esAddresses);
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
  }
  
  @Override
  public void process(DataStreamOperatorContext ctx, Message record) throws Exception {
    Event event = JSONSerializer.INSTANCE.fromBytes(record.getData(), Event.class) ;

    Calendar cal = Calendar.getInstance();
    cal.setTimeInMillis(event.getTimestamp().getTime());
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    
    ctx.write(record);
  }
  
  
}