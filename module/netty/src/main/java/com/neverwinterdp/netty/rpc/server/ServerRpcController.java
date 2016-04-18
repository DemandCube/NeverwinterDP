package com.neverwinterdp.netty.rpc.server;


import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

public class ServerRpcController implements RpcController {

  public void reset() {
  }

  @Override
  public boolean failed() {
    return false;
  }

  @Override
  public String errorText() {
    return null;
  }

  @Override
  public void startCancel() {
  }

  @Override
  public void setFailed(String reason) {
  }

  @Override
  public boolean isCanceled() {
    return false;
  }

  @Override
  public void notifyOnCancel(RpcCallback<Object> callback) {
  }
}