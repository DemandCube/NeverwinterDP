package com.neverwinterdp.netty.http;

import java.util.Map;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponse;

public interface StaticFileHandlerPlugin {
  public void init(Map<String, String> props) ;
  public void preProcess(ChannelHandlerContext ctx, FullHttpRequest request, String path) ;
  public void postProcess(ChannelHandlerContext ctx, FullHttpRequest request, HttpResponse response, String path) ;
}
