package com.neverwinterdp.netty.http.webapp;

import java.io.StringWriter;
import java.io.Writer;

import com.neverwinterdp.netty.http.RouteHandlerGeneric;
import com.neverwinterdp.util.ExceptionUtil;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;

public class WebAppRouteHandler extends RouteHandlerGeneric {
  final protected void doGet(ChannelHandlerContext ctx, HttpRequest request) {
    try {
      FullHttpRequest fullReq = (FullHttpRequest) request ;
      StringWriter writer = new StringWriter() ;
      process(writer, fullReq) ;
      writeContent(ctx, request, writer.toString(), "text/html") ;
      writer.close();
    } catch(Throwable t) {
      writeContent(ctx, request, ExceptionUtil.getStackTrace(t), "text/html") ;
      logger.error("Error", t);
    }
  }
  
  final protected void doPost(ChannelHandlerContext ctx, HttpRequest request) {
    try {
      FullHttpRequest fullReq = (FullHttpRequest) request ;
      StringWriter writer = new StringWriter() ;
      process(writer, fullReq) ;
      writeContent(ctx, request, writer.toString(), "text/html") ;
      writer.close();
    } catch(Throwable t) {
      writeContent(ctx, request, ExceptionUtil.getStackTrace(t), "text/html") ;
      logger.error("Error", t);
    }
  }
  
  protected void process(Writer writer, FullHttpRequest request) throws Exception {
    throw new RuntimeException("This method need to be implemented") ;
  }
}