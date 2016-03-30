package com.neverwinterdp.analytics.gripper;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import com.neverwinterdp.analytics.web.WebEvent;
import com.neverwinterdp.kafka.KafkaTool;
import com.neverwinterdp.kafka.producer.AckKafkaWriter;
import com.neverwinterdp.netty.http.client.ClientInfo;
import com.neverwinterdp.netty.http.client.ClientInfoCollectorHandler;
import com.neverwinterdp.util.ExceptionUtil;
import com.neverwinterdp.yara.Meter;
import com.neverwinterdp.yara.MetricRegistry;

public class ClientInfoCollectorHandlerExt extends ClientInfoCollectorHandler {
  private String     seedId    = UUID.randomUUID().toString();
  private AtomicLong idTracker = new AtomicLong();
  
  private MetricRegistry metricRegistry;
  private Meter          recordMeter;
  private Meter          byteMeter;
  
  private String         kafkaTopic;
  private AckKafkaWriter kafkaWriter;
  
  public ClientInfoCollectorHandlerExt(MetricRegistry metricRegistry, String zkConnects, String kafkaTopic) throws Exception {
    this.metricRegistry = metricRegistry;
    recordMeter = metricRegistry.meter("gripper.webpage.event.record");
    byteMeter   = metricRegistry.meter("gripper.webpage.event.byte");
   
    this.kafkaTopic = kafkaTopic;
    KafkaTool kafkaTool = new KafkaTool("KafkaClient", zkConnects);
    String kafkaConnects = kafkaTool.getKafkaBrokerList();
    kafkaWriter = new AckKafkaWriter("KafkaLogWriter", kafkaConnects) ;
  }
  
  @Override
  protected GripperAck onClientInfo(ClientInfo clientInfo, int dataSize) {
    //System.err.println("Client Info: " + JSONSerializer.INSTANCE.toString(clientInfo));
    WebEvent webEvent = new WebEvent();
    webEvent.setEventId(seedId + "-" + idTracker.incrementAndGet());
    webEvent.setTimestamp(System.currentTimeMillis());
    webEvent.setClientInfo(clientInfo);
    try {
      kafkaWriter.send(kafkaTopic, webEvent, 60000);
      recordMeter.mark(1);
      byteMeter.mark(dataSize);
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
