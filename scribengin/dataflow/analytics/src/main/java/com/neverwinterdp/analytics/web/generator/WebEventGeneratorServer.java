package com.neverwinterdp.analytics.web.generator;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.neverwinterdp.analytics.web.gripper.GripperAck;
import com.neverwinterdp.netty.http.client.AsyncHttpClient;
import com.neverwinterdp.netty.http.client.ResponseHandler;
import com.neverwinterdp.util.JSONSerializer;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;

public class WebEventGeneratorServer {
  private Logger logger = LoggerFactory.getLogger(WebEventGeneratorServer.class.getSimpleName());
  
  @Parameter(names = "--gripper-server-port", description = "The gripper port")
  private int    gripperServerPort = 7081;
  
  @Parameter(names = "--gripper-server-host", description = "The http port")
  private String gripperServerHost = "127.0.0.1";
  
  @Parameter(names = "--destination-topic", description = "")
  private String destinationTopic = "web.input";
  
  @ParametersDelegate
  private BrowserSessionGenerator browserSessionGenerator = new BrowserSessionGenerator();
  
  private int numOfThreads = 1;

  private ExecutorService executorService;
  
  public WebEventGeneratorServer() { }
  
  public WebEventGeneratorServer(String[] args) { 
    new JCommander(this, args);
  }
  
  public void start() throws Exception {
    browserSessionGenerator.start();
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
      BrowserSession session = null;
      int visitPageCount = 0 ;
      while((session = browserSessionGenerator.nextBrowserSession()) != null) {
        visitPageCount += session.sendWebEvent(client, destinationTopic);
        client.flush();
      }
      client.close();
    }
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
        ByteBuf bBuf = content.content();
        byte[] bytes = new byte[bBuf.readableBytes()];
        bBuf.readBytes(bytes);
        GripperAck ack = JSONSerializer.INSTANCE.fromBytes(bytes, GripperAck.class);
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
