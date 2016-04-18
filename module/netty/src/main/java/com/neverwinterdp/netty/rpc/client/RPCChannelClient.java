package com.neverwinterdp.netty.rpc.client;

import com.google.protobuf.BlockingRpcChannel;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcChannel;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.neverwinterdp.netty.rpc.protocol.Request;
import com.neverwinterdp.netty.rpc.protocol.Response;

public class RPCChannelClient implements RpcChannel, BlockingRpcChannel {
  private RPCClient client ;
  
  public RPCChannelClient(RPCClient client) {
    this.client = client;
  }
  
  public void callMethod(MethodDescriptor method, RpcController controller, 
                         Message request, Message responsePrototype, RpcCallback<Message> callback) {
    Request.Builder requestB = Request.newBuilder();
    requestB.setServiceId(method.getService().getFullName()) ;
    requestB.setMethodId(method.getName()) ;
    requestB.setParams(request.toByteString()) ;
    WirePayloadCallback payloadCallback = new WirePayloadCallback.NonBlocking(controller, callback, responsePrototype);
    client.call(requestB.build(), payloadCallback);
  }

  public Message callBlockingMethod(MethodDescriptor method, RpcController controller,
                                    Message request, Message responsePrototype) throws ServiceException {
    Request.Builder requestB = Request.newBuilder();
    requestB.setServiceId(method.getService().getFullName()) ;
    requestB.setMethodId(method.getName()) ;
    requestB.setParams(request.toByteString()) ;
    //System.out.println("Response: " + response) ;
    try {
      Response response = client.call(requestB.build());
      if(response.hasError()) {
        //System.out.println("Remote Service Error");
        //System.out.println("  message:    " + response.getError().getMessage());
        //System.out.println("  stacktrace: " + response.getError().getStacktrace());
        throw new ServiceException("Remote Service Error: " + response.getError().getMessage()) ;
      }
      Message ret = responsePrototype.getParserForType().parseFrom(response.getResult()) ;
      return ret ;
    } catch (Exception e) {
      throw new ServiceException(e) ;
    }
  }

}
