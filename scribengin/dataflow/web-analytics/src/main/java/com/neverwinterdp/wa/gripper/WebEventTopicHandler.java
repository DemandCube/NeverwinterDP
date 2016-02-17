package com.neverwinterdp.wa.gripper;

import java.util.concurrent.atomic.AtomicInteger;

import com.neverwinterdp.kafka.KafkaClient;
import com.neverwinterdp.kafka.producer.AckKafkaWriter;
import com.neverwinterdp.netty.http.rest.RestRouteHandler;
import com.neverwinterdp.util.ExceptionUtil;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.wa.event.WebEvent;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;

public class WebEventTopicHandler extends RestRouteHandler {
  static AtomicInteger counter = new AtomicInteger();
  
  private AckKafkaWriter kafkaWriter;
  
  public WebEventTopicHandler(String zkConnects) throws Exception {
    KafkaClient kafkaClient = new KafkaClient("KafkaClient", zkConnects);
    String kafkaConnects = kafkaClient.getKafkaBrokerList();
    kafkaWriter = new AckKafkaWriter("KafkaLogWriter", kafkaConnects) ;
  }
  
  protected Object get(ChannelHandlerContext ctx, FullHttpRequest request) {
    QueryStringDecoder reqDecoder = new QueryStringDecoder(request.getUri()) ;
    String path = reqDecoder.path() ;
    return new GripperAck("PingTopic Get, topic = " + path) ;
  }
  
  protected Object post(ChannelHandlerContext ctx, FullHttpRequest request) {
    QueryStringDecoder reqDecoder = new QueryStringDecoder(request.getUri()) ;
    String path = reqDecoder.path();
    String topic = path.substring(path.lastIndexOf('/') + 1);
    WebEvent webEvent = JSONSerializer.INSTANCE.fromBytes(getBodyData(request), WebEvent.class);
    try {
      kafkaWriter.send(topic, webEvent, 60000);
      return new GripperAck(webEvent.getEventId()) ;
    } catch (Exception e) {
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