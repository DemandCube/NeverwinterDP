package com.neverwinterdp.netty.rpc.client;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import com.neverwinterdp.netty.rpc.protocol.Request;
import com.neverwinterdp.netty.rpc.protocol.Response;
import com.neverwinterdp.netty.rpc.protocol.WirePayload;

public class RPCClientHandler extends SimpleChannelInboundHandler<WirePayload> {
  private AtomicLong correlationIdTracker = new AtomicLong() ;
  // Stateful properties
  private volatile Channel                 channel;
  private final ConcurrentMap<Long, WirePayloadCallback> callbacks = new ConcurrentHashMap<Long, WirePayloadCallback>() ;

  public RPCClientHandler() {
    super(false);
  }

  public <T> void call(Request request, WirePayloadCallback callback)  {
    WirePayload.Builder builder = WirePayload.newBuilder();
    builder.setCorrelationId(correlationIdTracker.incrementAndGet()) ;
    builder.setRequest(request);
    channel.writeAndFlush(builder.build());
    callbacks.put(builder.getCorrelationId(), callback);
  }
  
  public Response call(Request request) throws Exception {
    return call(request, 30000) ;
  }
  
  public Response call(Request request, long timeout) throws Exception {
    WirePayload.Builder builder = WirePayload.newBuilder();
    builder.setCorrelationId(correlationIdTracker.incrementAndGet()) ;
    builder.setRequest(request);
    channel.writeAndFlush(builder.build());
    WirePayloadCallback.Blocking callback = new WirePayloadCallback.Blocking() ;
    callbacks.put(builder.getCorrelationId(), callback);
    callback.waitForResult(timeout);
    
    WirePayload retPayload =callback.getPayload() ;
    if(retPayload != null) return retPayload.getResponse() ;
    throw new TimeoutException("Cannot get the response in 30s") ;
  }

  @Override
  public void channelRegistered(ChannelHandlerContext ctx) {
    channel = ctx.channel();
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, WirePayload payload) {
    WirePayloadCallback callback = callbacks.remove(payload.getCorrelationId()) ;
    if(callback != null) {
      try {
        callback.onReceive(payload);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    cause.printStackTrace();
    ctx.close();
  }
}