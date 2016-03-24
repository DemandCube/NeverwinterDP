package com.neverwinterdp.analytics.gripper;

import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import com.neverwinterdp.analytics.odyssey.MouseMoveEvent;
import com.neverwinterdp.kafka.KafkaTool;
import com.neverwinterdp.kafka.producer.AckKafkaWriter;
import com.neverwinterdp.netty.http.rest.RestRouteHandler;
import com.neverwinterdp.util.JSONSerializer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;

public class OdysseyMouseMoveEventCollectorHandler extends RestRouteHandler {
  private String         seedId  = UUID.randomUUID().toString();
  private AtomicLong     idTracker = new AtomicLong();
  
  private String         kafkaTopic;
  private AckKafkaWriter kafkaWriter;

  public OdysseyMouseMoveEventCollectorHandler(String zkConnects, String kafkaTopic) throws Exception {
    this.kafkaTopic = kafkaTopic;
    KafkaTool kafkaTool = new KafkaTool("KafkaClient", zkConnects);
    String kafkaConnects = kafkaTool.getKafkaBrokerList();
    kafkaWriter = new AckKafkaWriter("KafkaLogWriter", kafkaConnects) ;
    setContentType("text/plain");
  }

  protected Object get(ChannelHandlerContext ctx, FullHttpRequest request) {
    QueryStringDecoder reqDecoder = new QueryStringDecoder(request.getUri()) ;
    List<String> values = reqDecoder.parameters().get("jsonp");
    String jsonp = values.get(0);
    System.err.println("[OdysseyMouseMoveEventCollectorHandler]: jsonp length = " + jsonp.length());
    MouseMoveEvent event = JSONSerializer.INSTANCE.fromString(jsonp, MouseMoveEvent.class);
    return onOdysseyMouseMoveEvent(event);
  }

  protected Object post(ChannelHandlerContext ctx, FullHttpRequest request) {
    ByteBuf bBuf = request.content();
    byte[] bytes = new byte[bBuf.readableBytes()];
    bBuf.readBytes(bytes);
    System.err.println("[OdysseyMouseMoveEventCollectorHandler]: bytes length = " + bytes.length);
    MouseMoveEvent event = JSONSerializer.INSTANCE.fromBytes(bytes, MouseMoveEvent.class);
    return onOdysseyMouseMoveEvent(event);
  }

  protected Object onOdysseyMouseMoveEvent(MouseMoveEvent event) {
    try {
      event.setEventId("odyssey-mouse-move-" + seedId + "-" + idTracker.incrementAndGet());
      event.setTimestamp(new Date());
      kafkaWriter.send(kafkaTopic, event.getEventId(), event, 60000);
      return "{ success: true }";
    } catch (Exception e) {
      logger.error("Error:", e);
      return "{ success: false }";
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