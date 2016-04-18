package com.neverwinterdp.netty.http.webapp;

import static org.junit.Assert.assertEquals;
import io.netty.handler.codec.http.FullHttpRequest;

import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.netty.http.HttpServer;
import com.neverwinterdp.netty.http.client.DumpResponseHandler;
import com.neverwinterdp.netty.http.client.AsyncHttpClient;
/**
 * @author Tuan Nguyen
 * @email  tuan08@gmail.com
 */
public class WebAppUnitTest {
  private HttpServer server ;
  private int port =8181;
  
  @Before
  public void setup() throws Exception {
    server = new HttpServer();
    Map<String, String> props = new HashMap<String, String>();
    props.put("port", Integer.toString(port));
    server.configure(props);
    server.add("/hello", new HelloHandler());
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
  public void testHello() throws Exception {
    DumpResponseHandler handler = new DumpResponseHandler() ;
    AsyncHttpClient client = new AsyncHttpClient ("127.0.0.1", port, handler) ;
    client.get("/hello");
    
    Thread.sleep(1000);
    assertEquals(1, handler.getCount()) ;
  }
  
  static class Hello {
    private String message ;
    
    public Hello() { }
    
    public Hello(String mesg) { this.message = mesg ; }
    
    public String getMessage() { return this.message ; }
  }
  
  static public class HelloHandler extends WebAppRouteHandler {
    private HelloPage helloPage  ;
    
    public HelloHandler() throws Exception {
      helloPage = new HelloPage() ;
    }
    
    protected void process(Writer writer, FullHttpRequest request) throws Exception {
      Map<String, Object> scopes = new HashMap<String, Object>() ;
      helloPage.render(writer, scopes);
    }
  }
}
