package com.neverwinterdp.analytics.web.gripper;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import com.neverwinterdp.analytics.odyssey.ActionEvent;
import com.neverwinterdp.analytics.odyssey.MouseMoveEvent;
import com.neverwinterdp.es.ESClient;
import com.neverwinterdp.es.ESObjectClient;
import com.neverwinterdp.netty.http.rest.RestRouteHandler;
import com.neverwinterdp.util.JSONSerializer;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;

public class OdysseyMouseMoveEventListHandler extends RestRouteHandler {
  private ESObjectClient<ActionEvent>   esActionEventClient;

  public OdysseyMouseMoveEventListHandler(String[] esConnects) throws Exception {
    ESClient esClient = new ESClient(esConnects);
    esActionEventClient = new ESObjectClient<ActionEvent>(esClient, "odyssey-mouse-move", MouseMoveEvent.class) ;
    esActionEventClient.getESClient().waitForConnected(24 * 60 * 60 * 1000) ;
  }

  protected Object get(ChannelHandlerContext ctx, FullHttpRequest request) {
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
    List<MouseMoveEvent> events = new ArrayList<>();
    for(int i = 0; i < hits.getTotalHits(); i++) {
      SearchHit hit = hits.getAt(i);
      String json = hit.getSourceAsString();
      MouseMoveEvent event = JSONSerializer.INSTANCE.fromString(json, MouseMoveEvent.class);
      events.add(event);
    }
    return events;
  }

  
  public void close() {
    esActionEventClient.close();
  }
} 