package com.neverwinterdp.netty.http.rest;

import com.neverwinterdp.netty.http.RouteHandlerGeneric;
import com.neverwinterdp.util.ExceptionUtil;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;

public class RestRouteHandler extends RouteHandlerGeneric {
  final protected void doGet(ChannelHandlerContext ctx, HttpRequest request) {
    try {
      FullHttpRequest fullReq = (FullHttpRequest) request ;
      writeJSON(ctx, request, get(ctx, fullReq)) ;
    } catch(Throwable t) {
      writeJSON(ctx, request, ExceptionUtil.getStackTrace(t)) ;
      logger.error("Error", t);
    }
  }
  
  final protected void doPost(ChannelHandlerContext ctx, HttpRequest request) {
    try {
      FullHttpRequest fullReq = (FullHttpRequest) request ;
      writeJSON(ctx, request, post(ctx, fullReq)) ;
    } catch(Throwable t) {
      writeJSON(ctx, request, ExceptionUtil.getStackTrace(t)) ;
      logger.error("Error", t);
    }
  }
  
  protected Object get(ChannelHandlerContext ctx, FullHttpRequest request) {
    String stacktrace = ExceptionUtil.getStackTrace(new Exception("This method is not implemented")) ;
    return stacktrace ;
  }
  
  protected Object post(ChannelHandlerContext ctx, FullHttpRequest request) {
    String stacktrace = ExceptionUtil.getStackTrace(new Exception("This method is not implemented")) ;
    return stacktrace ;
  }
}