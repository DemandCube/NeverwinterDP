package com.neverwinterdp.netty.rpc.client;

import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.neverwinterdp.netty.rpc.protocol.Response;
import com.neverwinterdp.netty.rpc.protocol.WirePayload;

public interface WirePayloadCallback {
  public void onReceive(WirePayload payload) throws ServiceException ;

  static public class Blocking implements WirePayloadCallback {
    private WirePayload          payload;

    public WirePayload getPayload() { return this.payload ; }

    public void onReceive(WirePayload payload) throws ServiceException {
      this.payload = payload ;
      notifyOnResult() ;
    }

    synchronized public void waitForResult(long timeout) {
      try {
        this.wait(timeout);
      } catch (InterruptedException e) {
      }
    }

    synchronized public void notifyOnResult() {
      notify(); 
    }
  }
  
  static public class NonBlocking implements WirePayloadCallback {
    private RpcController controller ;
    private Message responsePrototype ;
    private RpcCallback<Message> callback ;
    
    public NonBlocking(RpcController controller, RpcCallback<Message> callback, Message responsePrototype) {
      this.controller = controller ;
      this.responsePrototype = responsePrototype ;
      this.callback = callback ;
    }
    
    public void onReceive(WirePayload payload) throws ServiceException {
      try {
        Response response = payload.getResponse();
        if(response.hasError()) {
          System.out.println("has error: ");
          System.out.println(response.getError().getStacktrace()) ;
          controller.setFailed(response.getError().getMessage());
          return ;
        }
        Message result = responsePrototype.getParserForType().parseFrom(response.getResult()) ;
        callback.run(result);
      } catch (Exception e) {
        controller.setFailed(e.getMessage());
      }
    }
  }
}