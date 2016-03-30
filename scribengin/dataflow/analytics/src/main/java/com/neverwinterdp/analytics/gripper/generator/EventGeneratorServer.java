package com.neverwinterdp.analytics.gripper.generator;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.neverwinterdp.analytics.gripper.GripperAck;
import com.neverwinterdp.netty.http.client.AsyncHttpClient;
import com.neverwinterdp.netty.http.client.ResponseHandler;
import com.neverwinterdp.util.JSONSerializer;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;

public class EventGeneratorServer {
  private Logger logger = LoggerFactory.getLogger(EventGeneratorServer.class.getSimpleName());
  
  @Parameter(names = "--gripper-server-port", description = "The gripper port")
  private int    gripperServerPort = 7081;
  
  @Parameter(names = "--gripper-server-host", description = "The http port")
  private String gripperServerHost = "127.0.0.1";
  
  @Parameter(names = "--destination-topic", description = "")
  private String destinationTopic = "web.input";
  
  @ParametersDelegate
  private ClientSessionManager clientSessionManager = new ClientSessionManager();
  
  @Parameter(names = "--num-of-threads", description = "")
  private int numOfThreads = 1;

  private ExecutorService executorService;
  
  public EventGeneratorServer() { }
  
  public EventGeneratorServer(String[] args) { 
    new JCommander(this, args);
  }
  
  public void start() throws Exception {
    clientSessionManager.start();
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
      ClientSession session = null;
      int count = 0;
      Random rand = new Random();
      while((session = clientSessionManager.takeClientSession()) != null) {
        session.sendWebEvent(client, "/rest/client/info.collector");
        count++ ;
        if(count % 311 == 0) {
          for(int i = 0; i < rand.nextInt(10); i++) {
            session.sendADSEvent(client, "/rest/client/ads-event.collector");
          }
          client.flush();
        }
        if(session.hasNextWebEvent()) {
          clientSessionManager.releaseClientSession(session);
        }
        Thread.sleep(rand.nextInt(10) + 1);
      }
      client.flush();
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
