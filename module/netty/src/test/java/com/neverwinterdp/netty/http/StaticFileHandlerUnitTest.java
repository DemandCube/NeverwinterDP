package com.neverwinterdp.netty.http;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponse;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.netty.http.client.AsyncHttpClient;
import com.neverwinterdp.netty.http.client.DumpResponseHandler;
/**
 * @author Tuan Nguyen
 * @email  tuan08@gmail.com
 */
public class StaticFileHandlerUnitTest {
  private HttpServer server ;
  
  @Before
  public void setup() throws Exception {
    Map<String, String> props = new HashMap<String, String>() ;
    props.put("port","8080") ;
    props.put("www-dir", ".") ;
    props.put("static-file-handler.plugins", "sample") ;
    props.put("static-file-handler.sample.class", SampleRequestHeaderPlugin.class.getName()) ;
    server = new HttpServer() ;
    server.configure(props);
    server.startAsDeamon() ;
    Thread.sleep(2000) ;
  }
  
  @After
  public void teardown() {
    server.shutdown() ;
  }
  
  @Test
  public void testStaticFileHandler() throws Exception {
    DumpResponseHandler handler = new DumpResponseHandler() ;
    AsyncHttpClient client = new AsyncHttpClient ("127.0.0.1", 8080, handler) ;
    client.get("/build.gradle");
    Thread.sleep(100) ;
  }
  
  static public class SampleRequestHeaderPlugin implements StaticFileHandlerPlugin {
    public void init(Map<String, String> props) {
    }

    public void preProcess(ChannelHandlerContext ctx, FullHttpRequest request, String path) {
      System.out.println("pre process path = " + path);
    }

    public void postProcess(ChannelHandlerContext ctx, FullHttpRequest request, HttpResponse response, String path) {
      System.out.println("post process path = " + path);
      System.out.println("Request Header: ");
      Iterator<Entry<String, String>> i = request.headers().iterator() ;
      while(i.hasNext()) {
        Entry<String, String> entry =i.next();
        System.out.println("  " + entry.getKey() + ": " + entry.getValue());
      }
      
      System.out.println("Response Header: ");
      i = response.headers().iterator() ;
      while(i.hasNext()) {
        Entry<String, String> entry =i.next();
        System.out.println("  " +  entry.getKey() + ": " + entry.getValue());
      }
    }
    
  }
}
