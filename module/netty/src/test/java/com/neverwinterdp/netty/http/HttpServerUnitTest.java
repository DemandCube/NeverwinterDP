package com.neverwinterdp.netty.http;

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.netty.http.client.AsyncHttpClient;
import com.neverwinterdp.netty.http.client.DumpResponseHandler;
/**
 * @author Tuan Nguyen
 * @email  tuan08@gmail.com
 */
public class HttpServerUnitTest {
  private HttpServer server ;
  
  @Before
  public void setup() throws Exception {
    server = new HttpServer();
    server.add("/ping", new PingRouteHandler());
    new Thread() {
      public void run() {
        try {
          server.start() ;
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }.start() ;
    Thread.sleep(1000);
  }
  
  @After
  public void teardown() {
    server.shutdown() ;
  }
  
  @Test
  public void testGet() throws Exception {
    DumpResponseHandler handler = new DumpResponseHandler() ;
    AsyncHttpClient client = new AsyncHttpClient ("127.0.0.1", 8080, handler) ;
    for(int i = 0; i < 10; i++) {
      if(i % 2 == 0) client.get("/ping");
      else client.post("/ping", "Hello");
    }
    Thread.sleep(1000);
    assertEquals(10, handler.getCount()) ;
  }
}
