package com.neverwinterdp.netty.http;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpRequest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.netty.http.client.AsyncHttpClient;
import com.neverwinterdp.netty.http.client.DumpResponseHandler;
/**
 * @author Tuan Nguyen
 * @email  tuan08@gmail.com
 */
public class HttpConnectionUnitTest {
  private HttpServer server ;
  
  @Before
  public void setup() throws Exception {
    server = new HttpServer();
    server.add("/longtask", new LongTaskRouteHandler());
    server.startAsDeamon() ;
    Thread.sleep(1000);
  }
  
  @After
  public void teardown() {
    server.shutdown() ;
  }
  
  @Test
  public void testLongTask() throws Exception {
    DumpResponseHandler handler = new DumpResponseHandler() ;
    AsyncHttpClient client = new AsyncHttpClient ("127.0.0.1", 8080, handler) ;
    for(int i = 0; i < 3; i++) {
      client.get("/longtask") ;
    }
    client.close() ;
    Thread.sleep(3000);
  }
  
  static public class LongTaskRouteHandler extends RouteHandlerGeneric {
    @Override
    protected void doGet(ChannelHandlerContext ctx, HttpRequest httpReq) {
      System.out.println("start doGet()");
      try {
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      writeContent(ctx, httpReq, "long task", "text/plain");
      System.out.println("finish doGet()");
    }
  }
}
