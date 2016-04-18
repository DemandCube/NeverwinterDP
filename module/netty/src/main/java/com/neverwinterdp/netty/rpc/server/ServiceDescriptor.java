package com.neverwinterdp.netty.rpc.server;

import com.google.protobuf.BlockingService;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;

abstract public class ServiceDescriptor<T> {
  final protected T service;
  final protected boolean allowTimeout;

  public ServiceDescriptor( boolean allowTimeout, T s ) {
    this.service = s;
    this.allowTimeout = allowTimeout;
  }

  public T getService() { return service; }

  public boolean isAllowTimeout() { return allowTimeout; }
  
  abstract public MethodDescriptor findMethodByName(String name) ;
  
  abstract public Message getRequestPrototype(MethodDescriptor methodDesc) ;
  
  abstract public Message invoke(RpcController controller, MethodDescriptor methodDesc,Message request) throws ServiceException ;
  
  static public class BlockingServiceDescriptor extends ServiceDescriptor<BlockingService> {
    public BlockingServiceDescriptor(boolean allowTimeout, BlockingService s) {
      super(allowTimeout, s);
    }

    public MethodDescriptor findMethodByName(String name) {
      return service.getDescriptorForType().findMethodByName(name);
    }

    @Override
    public Message getRequestPrototype(MethodDescriptor methodDesc) {
      return service.getRequestPrototype(methodDesc);
    }
    
    public Message invoke(RpcController controller, MethodDescriptor methodDesc, Message request) throws ServiceException {
      return service.callBlockingMethod(methodDesc, controller, request) ;
    }
  }
  
  static public class NonBlockingServiceDescriptor extends ServiceDescriptor<Service> {
    public NonBlockingServiceDescriptor(boolean allowTimeout, Service s) {
      super(allowTimeout, s);
    }
    
    public MethodDescriptor findMethodByName(String name) {
      return service.getDescriptorForType().findMethodByName(name);
    }
    
    @Override
    public Message getRequestPrototype(MethodDescriptor methodDesc) {
      return service.getRequestPrototype(methodDesc);
    }
    
    public Message invoke(RpcController controller, MethodDescriptor methodDesc, Message request) throws ServiceException {
      RpcCallbackImpl callback = new RpcCallbackImpl() ;
      service.callMethod(methodDesc, controller, request, callback) ;
      if(callback.result == null) {
        callback.waitForResult(10000);
      }
      if(callback.result == null) {
        throw new ServiceException("Timeout, there is no result") ;
      }
      return callback.result ;
    }
    
    static class RpcCallbackImpl implements RpcCallback<Message> {
      Message result ;
      public void run(Message parameter) {
        result = parameter ;
        notifyOnResult() ;
      }
      
      synchronized void waitForResult(long timeout) {
        try {
          wait(timeout) ;
        } catch (InterruptedException e) {
        }
      }
      
      synchronized void notifyOnResult() {
        notify() ;
      }
    }
  }
}