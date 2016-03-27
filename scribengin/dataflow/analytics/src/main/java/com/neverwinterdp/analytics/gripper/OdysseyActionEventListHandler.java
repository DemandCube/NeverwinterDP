package com.neverwinterdp.analytics.gripper;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import com.neverwinterdp.analytics.odyssey.ActionEvent;
import com.neverwinterdp.es.ESClient;
import com.neverwinterdp.es.ESObjectClient;
import com.neverwinterdp.netty.http.rest.RestRouteHandler;
import com.neverwinterdp.util.JSONSerializer;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;

public class OdysseyActionEventListHandler extends RestRouteHandler {
  private ESObjectClient<ActionEvent>   esActionEventClient;

  public OdysseyActionEventListHandler(String[] esConnects) throws Exception {
    ESClient esClient = new ESClient(esConnects);
    esActionEventClient = new ESObjectClient<ActionEvent>(esClient, "analytics-odyssey-action-event", ActionEvent.class) ;
    esActionEventClient.getESClient().waitForConnected(24 * 60 * 60 * 1000) ;
  }

  protected Object get(ChannelHandlerContext ctx, FullHttpRequest request) {
    //System.err.println("[OdysseyActionEventListHandler] uri = " + request.getUri());
    QueryStringDecoder reqDecoder = new QueryStringDecoder(request.getUri()) ;
    List<String> values = reqDecoder.parameters().get("source");
    String source = "all";
    if(values.size() > 0) source = values.get(0);
    SearchResponse searchResp = null;
    if("user".equals(source)) {
      searchResp  = esActionEventClient.searchTerm("source", "user", 0, 1000);
    } else {
      searchResp  = esActionEventClient.searchTermByRegex("eventId", ".*", 0, 1000);
    }
    SearchHits hits = searchResp.getHits();
    List<ActionEvent> events = new ArrayList<>();
    for(int i = 0; i < hits.getTotalHits(); i++) {
      SearchHit hit = hits.getAt(i);
      String json = hit.getSourceAsString();
      System.err.println("[OdysseyActionEventListHandler] " + json);
      ActionEvent event = JSONSerializer.INSTANCE.fromString(json, ActionEvent.class);
      events.add(event);
    }
    return events;
  }

  
  public void close() {
    esActionEventClient.close();
  }
} 