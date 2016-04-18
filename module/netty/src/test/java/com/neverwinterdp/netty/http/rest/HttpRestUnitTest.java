package com.neverwinterdp.netty.http.rest;

import static org.junit.Assert.assertEquals;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.netty.http.HttpServer;
import com.neverwinterdp.netty.http.client.DumpResponseHandler;
import com.neverwinterdp.netty.http.client.AsyncHttpClient;
import com.neverwinterdp.netty.http.rest.RestRouteHandler;
/**
 * @author Tuan Nguyen
 * @email  tuan08@gmail.com
 */
public class HttpRestUnitTest {
  private HttpServer server ;
  
  @Before
  public void setup() throws Exception {
    server = new HttpServer();
    server.add("/ping", new Ping());
    server.add("/ping/:topic", new PingTopic());
    server.startAsDeamon();
    Thread.sleep(1000);
  }
  
  @After
  public void teardown() {
    server.shutdown() ;
  }
  
  @Test
  public void testPing() throws Exception {
    DumpResponseHandler handler = new DumpResponseHandler() ;
    AsyncHttpClient client = new AsyncHttpClient ("127.0.0.1", 8080, handler) ;
    client.get("/ping");
    client.post("/ping", "Hello");
    
    client.get("/ping/topic1");
    client.post("/ping/topic2", "Hello Topic 2");
    
    Thread.sleep(1000);
    assertEquals(4, handler.getCount()) ;
  }
  
  static class Hello {
    private String message ;
    
    public Hello() { }
    
    public Hello(String mesg) { this.message = mesg ; }
    
    public String getMessage() { return this.message ; }
  }
  
  static public class Ping extends RestRouteHandler {
    protected Object get(ChannelHandlerContext ctx, FullHttpRequest request) {
      return new Hello("HelloHandler Get") ;
    }
    
    protected Object post(ChannelHandlerContext ctx, FullHttpRequest request) {
      String data = new String(getBodyData(request)) ;
      return new Hello("HelloHandler Post, Data = " + data) ;
    }
  }
  
  static public class PingTopic extends RestRouteHandler {
    protected Object get(ChannelHandlerContext ctx, FullHttpRequest request) {
      QueryStringDecoder reqDecoder = new QueryStringDecoder(request.getUri()) ;
      String path = reqDecoder.path() ;
      return new Hello("PingTopic Get, topic = " + path) ;
    }
    
    protected Object post(ChannelHandlerContext ctx, FullHttpRequest request) {
      QueryStringDecoder reqDecoder = new QueryStringDecoder(request.getUri()) ;
      String path = reqDecoder.path() ;
      String data = new String(getBodyData(request)) ;
      return new Hello("HelloHandler Post, Data = " + data + ", topic = " + path) ;
    }
  }
}
