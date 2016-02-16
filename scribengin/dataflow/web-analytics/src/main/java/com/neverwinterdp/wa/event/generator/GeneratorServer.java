package com.neverwinterdp.wa.event.generator;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.neverwinterdp.netty.http.client.AsyncHttpClient;
import com.neverwinterdp.netty.http.client.ResponseHandler;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.wa.event.BrowserInfo;
import com.neverwinterdp.wa.event.WebEvent;
import com.neverwinterdp.wa.gripper.GripperAck;

import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;

public class GeneratorServer {
  private Logger logger = LoggerFactory.getLogger(GeneratorServer.class.getSimpleName());
  
  @Parameter(names = "--gripper-server-port", description = "The gripper port")
  private int    gripperServerPort = 8080;
  
  @Parameter(names = "--gripper-server-host", description = "The http port")
  private String gripperServerHost = "127.0.0.1";
  
  private int numOfThreads = 1;
  
  private int numOfMessages = 10000;
  
  private ExecutorService executorService;
  
  public GeneratorServer setNumOfMessages(int num) {
    numOfMessages = num;
    return this;
  }
  
  public void start() throws Exception {
    executorService = Executors.newFixedThreadPool(numOfThreads);
    for(int i = 0; i < numOfThreads; i++) {
      executorService.submit(new GeneratorWorker());
    }
    executorService.shutdown();
  }

  public void shutdown() throws Exception {
    executorService.shutdownNow();
  }
  
  public class GeneratorWorker implements Runnable {
    public void run() {
      try {
        doRun();
      } catch (Exception ex) {
        logger.error("Error: ", ex);
      }
    }
    
    public void doRun() throws Exception {
      GripperResponseHandler handler = new GripperResponseHandler() ;
      AsyncHttpClient client = new AsyncHttpClient (gripperServerHost, gripperServerPort, handler) ;
      WebEventGenerator webEventGenerator = new WebEventGenerator();
      BrowserInfo browser = Browsers.createMacbookAirBrowser();
      for(int i = 0; i < numOfMessages; i++) {
        WebEvent wEvent = webEventGenerator.next(browser, "user.click", "GET", "http://google.com");
        client.post("/webevent/user.click", wEvent);
      }
    }
  }
  
  public class BrowserSession {
    private BrowserInfo browserInfo;
  }
  
  public class GripperResponseHandler implements ResponseHandler {
    @Override
    public void onResponse(HttpResponse response) {
      HttpResponseStatus status = response.getStatus();
      if(status.code() != 200) {
        logger.error("Expect http response status = 200, but " + status.code());
        return;
      }
      
      if(response instanceof HttpContent) {
        HttpContent content = (HttpContent) response;
        GripperAck ack = JSONSerializer.INSTANCE.fromBytes(content.content().array(), GripperAck.class);
        if(!ack.isSuccess()) {
          logger.error("Get a failed ack from gripper server!");
          logger.error(ack.getErrorMessage());
        }
      }
    }
  }
  
  static public void main(String[] args) throws Exception {
  }
}
