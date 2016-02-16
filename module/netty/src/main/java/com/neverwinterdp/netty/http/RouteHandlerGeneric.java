package com.neverwinterdp.netty.http;

import static io.netty.handler.codec.http.HttpHeaders.isKeepAlive;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED ;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.util.Map;

import org.slf4j.Logger;

import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.util.text.StringUtil;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.HttpHeaders.Values;
/**
 * @author Tuan Nguyen
 * @email  tuan08@gmail.com
 */
public class RouteHandlerGeneric implements RouteHandler {
  protected Logger logger ;
  protected JSONSerializer jsonSerializer = JSONSerializer.INSTANCE ;
  
  public void configure(Map<String, String> props) {
  }
  
  public JSONSerializer getJSONSerializer() { return this.jsonSerializer ; }
  
  public void setJSONSerializer(JSONSerializer jsonSerializer) {
    this.jsonSerializer = jsonSerializer ;
  }
  
  public void setLogger(Logger logger) {
    this.logger = logger ;
  }

  final public void handle(ChannelHandlerContext ctx, HttpRequest request) {
    HttpMethod method = request.getMethod() ;
    if(HttpMethod.GET.equals(method)) {
      doGet(ctx, request) ;
    } else if(HttpMethod.POST.equals(method)) {
      doPost(ctx, request) ;
    } else {
      unsupport(ctx, request);
    }
  }
  
  protected void doGet(ChannelHandlerContext ctx, HttpRequest request) {
    unsupport(ctx, request);
  }
  
  protected void doPost(ChannelHandlerContext ctx, HttpRequest request) {
    unsupport(ctx, request); 
  }
  
  protected void unsupport(ChannelHandlerContext ctx, HttpRequest request) {
    String message = "The method " + request.getMethod() + " is not supportted";
    ByteBuf contentBuf = Unpooled.wrappedBuffer(message.getBytes()) ;
    FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, METHOD_NOT_ALLOWED, contentBuf);
    response.headers().set(CONTENT_TYPE, "text/plain");
    response.headers().set(CONTENT_LENGTH, response.content().readableBytes());
    ctx.write(response).addListener(ChannelFutureListener.CLOSE);
    contentBuf.release() ;
    logger.info(message);
  }
  
  protected <T> void writeJSON(ChannelHandlerContext ctx, HttpRequest req, T obj) {
    byte[] data = jsonSerializer.toBytes(obj) ;
    ByteBuf content = Unpooled.wrappedBuffer(data) ;
    writeContent(ctx, req, content, "application/json") ;
  }
  
  protected <T> void writeBytes(ChannelHandlerContext ctx, HttpRequest req, byte[] bytes) {
    ByteBuf content = Unpooled.wrappedBuffer(bytes) ;
    writeContent(ctx, req, content, "application/binary") ;
  }
  
  protected <T> void writeContent(ChannelHandlerContext ctx, HttpRequest req, String content, String mimeType) {
    FullHttpResponse response = createResponse(req, content, mimeType);
    write(ctx, req, response) ;
  }
  
  protected void writeContent(ChannelHandlerContext ctx, HttpRequest req, ByteBuf content, String mimeType) {
    FullHttpResponse response = createResponse(req, content, mimeType);
    write(ctx, req, response) ;
  }
  
  protected FullHttpResponse createResponse(HttpRequest req, String content, String mimeType) {
    byte[] data = content.getBytes(StringUtil.UTF8) ;
    ByteBuf bBuf = Unpooled.wrappedBuffer(data) ;
    return createResponse(req, bBuf, mimeType) ;
  }
  
  protected FullHttpResponse createResponse(HttpRequest req, ByteBuf content, String mimeType) {
    FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, OK, content);
    response.headers().set(CONTENT_TYPE, mimeType);
    response.headers().set(CONTENT_LENGTH, response.content().readableBytes());
    response.headers().set(HttpHeaders.Names.ACCEPT_ENCODING, HttpHeaders.Values.GZIP);
    return response ;
  }
  
  protected void write(ChannelHandlerContext ctx, HttpRequest request, FullHttpResponse response) {
    boolean keepAlive = isKeepAlive(request);
    if (!keepAlive) {
      ctx.write(response).addListener(ChannelFutureListener.CLOSE);
    } else {
      response.headers().set(CONNECTION, Values.KEEP_ALIVE);
      ctx.write(response) ;
    }
    ctx.flush() ;
  }
  
  public byte[] getBodyData(FullHttpRequest req) {
    ByteBuf byteBuf = req.content() ;
    byte[] bytes = new byte[byteBuf.readableBytes()] ;
    byteBuf.readBytes(bytes) ;
    return bytes ;
  }
  
  public void close() {
  }
}