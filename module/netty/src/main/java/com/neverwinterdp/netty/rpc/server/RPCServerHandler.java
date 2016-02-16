package com.neverwinterdp.netty.rpc.server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.ServiceException;
import com.neverwinterdp.netty.rpc.protocol.Request;
import com.neverwinterdp.netty.rpc.protocol.Response;
import com.neverwinterdp.netty.rpc.protocol.WirePayload;

public class RPCServerHandler extends SimpleChannelInboundHandler<WirePayload> {
  private ServiceRegistry registry ;
  
  public RPCServerHandler(ServiceRegistry registry) {
    this.registry = registry ;
  }
  
  @Override
  protected void channelRead0(ChannelHandlerContext ctx, WirePayload payload) {
    WirePayload.Builder payloadB = WirePayload.newBuilder();
    payloadB.setCorrelationId(payload.getCorrelationId()) ;
    Response.Builder responseB = Response.newBuilder() ;
    invoke(payload.getRequest(), responseB) ;
    payloadB.setResponse(responseB.build()) ;
    ctx.write(payloadB.build());
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) {
    ctx.flush();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    cause.printStackTrace();
    ctx.close();
  }
  
  private void invoke(Request request, Response.Builder responseB) {
    ServiceDescriptor<?> sd = registry.getService(request.getServiceId()) ;
    if(sd == null) {
      responseB.setError(Errors.serviceNotFound(new Throwable("Service " + request.getServiceId()))) ;
      return  ;
    }
    
    MethodDescriptor methodDesc = sd.findMethodByName(request.getMethodId());
    if(methodDesc == null)  {
      String mesg = "Method " + request.getMethodId() + " is not found for service " + request.getServiceId() ;
      responseB.setError(Errors.methodNotFound(new Throwable(mesg))) ;
      return  ;
    }
    Message param = null ;
    try {
      Message requestPrototype = sd.getRequestPrototype(methodDesc);
      param = requestPrototype.getParserForType().parseFrom(request.getParams());
    } catch (InvalidProtocolBufferException e) {
      responseB.setError(Errors.methodInvalidParam(e)) ;
      return  ;
    }
    try {
      ServerRpcController controller = new ServerRpcController() ;
      Message result = sd.invoke(controller, methodDesc, param) ;
      responseB.setResult(result.toByteString()) ;
    } catch (ServiceException e) {
      responseB.setError(Errors.methodInvokeError(e)) ;
    }
  }
}
