package com.neverwinterdp.scribengin.webui;

import java.util.List;
import java.util.Map;

import com.neverwinterdp.netty.http.rest.RestRouteHandler;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.util.ExceptionUtil;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;

public class RestRequestHandler extends RestRouteHandler {
  private ScribenginClient scribenginClient;
  protected CommandMapping commandMapping = new CommandMapping();
  
  
  protected RestRequestHandler(ScribenginClient scribenginClient) {
    this.scribenginClient = scribenginClient;
  }
  
  protected void addCommand(String name, Class<? extends Command> type) {
    commandMapping.add(name, type);
  }
  
  protected Object get(ChannelHandlerContext ctx, FullHttpRequest request) {
    return executeGet(ctx, request);
  }
  
  protected Object post(ChannelHandlerContext ctx, FullHttpRequest request) {
    return "ok";
  }
  
  protected CommandResponse executeGet(ChannelHandlerContext ctx, FullHttpRequest request) {
    QueryStringDecoder reqDecoder = new QueryStringDecoder(request.getUri()) ;
    Map<String, List<String>> params = reqDecoder.parameters();
    String path = reqDecoder.path() ;
    int slashLastIdx = path.lastIndexOf('/');
    String cmdName = path.substring(slashLastIdx + 1);
    CommandResponse cmdResponse = new CommandResponse(cmdName);
    try {
      Command command = commandMapping.getInstance(cmdName, params);
      CommandContext context = new CommandContext(scribenginClient);
      Object result = command.execute(context);
      cmdResponse.setResultAs(result);
    } catch(Throwable e) {
      System.err.println("path uri = " + request.getUri());
      e.printStackTrace();
      cmdResponse.setError(ExceptionUtil.getStackTrace(e));
    }
    return cmdResponse;
  }
  
  public void close() {
  }
} 