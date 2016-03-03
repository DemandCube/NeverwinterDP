package com.neverwinterdp.analytics.web.gripper;

import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.neverwinterdp.analytics.web.WebEvent;
import com.neverwinterdp.kafka.KafkaTool;
import com.neverwinterdp.kafka.producer.AckKafkaWriter;
import com.neverwinterdp.netty.http.client.ClientInfo;
import com.neverwinterdp.netty.http.client.ClientInfoCollectorHandler;
import com.neverwinterdp.util.ExceptionUtil;
import com.neverwinterdp.util.JSONSerializer;

public class ClientInfoCollectorHandlerExt extends ClientInfoCollectorHandler {
  private String     seedId = UUID.randomUUID().toString();
  private AtomicLong idTracker = new AtomicLong();
  
  private Map<String, WebEvent> webEventBuffer = new ConcurrentHashMap<String, WebEvent>();
  
  private String kafkaTopic;
  private AckKafkaWriter kafkaWriter;
  private FlushThread flushThread ;
  
  public ClientInfoCollectorHandlerExt(String zkConnects, String kafkaTopic) throws Exception {
    this.kafkaTopic = kafkaTopic;
    KafkaTool kafkaTool = new KafkaTool("KafkaClient", zkConnects);
    String kafkaConnects = kafkaTool.getKafkaBrokerList();
    kafkaWriter = new AckKafkaWriter("KafkaLogWriter", kafkaConnects) ;
    setContentType("text/plain");
    flushThread = new FlushThread();
    flushThread.start();
  }
  
  protected GripperAck onClientInfo(ClientInfo clientInfo) {
    System.err.println("Client Info: " + JSONSerializer.INSTANCE.toString(clientInfo));
    WebEvent webEvent = new WebEvent();
    webEvent.setTimestamp(System.currentTimeMillis());
    webEvent.setEventId(seedId + "-" + idTracker.incrementAndGet());
    webEvent.setClientInfo(clientInfo);
    try {
      WebEvent prevWebEvent = webEventBuffer.get(clientInfo.user.visitorId);
      if(prevWebEvent != null) {
        prevWebEvent.getClientInfo().user.spentTime = webEvent.getTimestamp() - prevWebEvent.getTimestamp();
        kafkaWriter.send(kafkaTopic, prevWebEvent, 60000);
      } 
      webEventBuffer.put(clientInfo.user.visitorId, webEvent);
      
      return new GripperAck(webEvent.getEventId()) ;
    } catch (Exception e) {
      logger.error("Error: ", e);
      return new GripperAck(webEvent.getEventId(), ExceptionUtil.getStackTrace(e)) ;
    }
  }
  
  public void close() {
    try {
      flushThread.terminate = true;
      flushThread.interrupt();
      kafkaWriter.close();
    } catch (InterruptedException e) {
      logger.error("Error: ", e);
    }
  }
  
  public class FlushThread extends Thread {
    private boolean terminate = false;
    public void run() {
      while(!terminate) {
        try {
          Thread.sleep(5000);
          flush();
        } catch (Exception e) {
          logger.error("flush error: ", e);
        }
      }
    }
    
    public void flush() throws Exception {
      long currentTime = System.currentTimeMillis();
      Iterator<WebEvent> i = webEventBuffer.values().iterator();
      while(i.hasNext()) {
        WebEvent webEvent = i.next();
        if(currentTime - webEvent.getTimestamp() > 30000) {
          webEvent.getClientInfo().user.spentTime = 30000;
          kafkaWriter.send(kafkaTopic, webEvent, 60000);
          i.remove();
        }
      }
    }
  }
}
