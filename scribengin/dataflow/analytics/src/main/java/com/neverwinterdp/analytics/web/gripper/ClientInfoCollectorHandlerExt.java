package com.neverwinterdp.analytics.web.gripper;

import com.neverwinterdp.analytics.web.WebEvent;
import com.neverwinterdp.kafka.KafkaTool;
import com.neverwinterdp.kafka.producer.AckKafkaWriter;
import com.neverwinterdp.netty.http.client.ClientInfo;
import com.neverwinterdp.netty.http.client.ClientInfoCollectorHandler;
import com.neverwinterdp.util.ExceptionUtil;
import com.neverwinterdp.util.JSONSerializer;

public class ClientInfoCollectorHandlerExt extends ClientInfoCollectorHandler {
  private String kafkaTopic;
  private AckKafkaWriter kafkaWriter;
  
  public ClientInfoCollectorHandlerExt(String zkConnects, String kafkaTopic) throws Exception {
    this.kafkaTopic = kafkaTopic;
    KafkaTool kafkaTool = new KafkaTool("KafkaClient", zkConnects);
    String kafkaConnects = kafkaTool.getKafkaBrokerList();
    kafkaWriter = new AckKafkaWriter("KafkaLogWriter", kafkaConnects) ;
    setContentType("text/plain");
  }
  
  protected GripperAck onClientInfo(ClientInfo clientInfo) {
    System.err.println("data: " + JSONSerializer.INSTANCE.toString(clientInfo));
    WebEvent webEvent = new WebEvent();
    webEvent.setClientInfo(clientInfo);
    try {
      kafkaWriter.send(kafkaTopic, webEvent, 60000);
      return new GripperAck(webEvent.getEventId()) ;
    } catch (Exception e) {
      logger.error("Error: ", e);
      return new GripperAck(webEvent.getEventId(), ExceptionUtil.getStackTrace(e)) ;
    }
  }
  
  public void close() {
    try {
      kafkaWriter.close();
    } catch (InterruptedException e) {
      logger.error("Error: ", e);
    }
  }
}
