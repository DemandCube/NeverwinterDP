package com.neverwinterdp.netty.http.client;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;

import com.neverwinterdp.netty.http.rest.RestRouteHandler;
import com.neverwinterdp.util.ExceptionUtil;
import com.neverwinterdp.util.JSONSerializer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;

public class ClientInfoCollectorHandler extends RestRouteHandler {
  
  public ClientInfoCollectorHandler() {
  }
  
  protected Object get(ChannelHandlerContext ctx, FullHttpRequest request) {
    QueryStringDecoder reqDecoder = new QueryStringDecoder(request.getUri()) ;
    List<String> values = reqDecoder.parameters().get("jsonp");
    String jsonp = values.get(0);
    ClientInfo clientInfo = JSONSerializer.INSTANCE.fromString(jsonp, ClientInfo.class);
    clientInfo.user.ipAddress = getIpAddress(ctx);
    return onClientInfo(clientInfo);
  }
  
  protected Object post(ChannelHandlerContext ctx, FullHttpRequest request) {
    try {
      ByteBuf bBuf = request.content();
      byte[] bytes = new byte[bBuf.readableBytes()];
      bBuf.readBytes(bytes);
      ClientInfo clientInfo = JSONSerializer.INSTANCE.fromBytes(bytes, ClientInfo.class);
      clientInfo.user.ipAddress = getIpAddress(ctx);
      return onClientInfo(clientInfo);
    } catch(Throwable t) {
      return ExceptionUtil.getStackTrace(t);
    }
  }
  
  protected Object onClientInfo(ClientInfo clientInfo) {
    return "{}";
  }
  
  String getIpAddress(ChannelHandlerContext ctx) {
    InetSocketAddress socketAddress = (InetSocketAddress) ctx.channel().remoteAddress();
    InetAddress inetaddress = socketAddress.getAddress();
    String ipAddress = inetaddress.getHostAddress(); 
    return ipAddress;
  }
} 