package com.neverwinterdp.netty.rpc.ping;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.neverwinterdp.netty.rpc.ping.protocol.Ping;
import com.neverwinterdp.netty.rpc.ping.protocol.PingService;
import com.neverwinterdp.netty.rpc.ping.protocol.Pong;

public class PingServiceImpl implements PingService.BlockingInterface, PingService.Interface {
  @Override
  public Pong ping(RpcController controller, Ping request) throws ServiceException {
    System.out.println("PingServiceImpl: blocking ping");
    Pong response = Pong.newBuilder().setMessage("Pong From PingServiceImpl").build();
    return response;
  }
  
  @Override
  public void ping(RpcController controller, Ping request, RpcCallback<Pong> done) {
    System.out.println("PingServiceImpl: non blocking ping");
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
    }
    if ( controller.isCanceled() ) {
      done.run(null);
      return;
    }
    Pong pong = Pong.newBuilder().setMessage("Pong From NonBlockingPingServer").build();
    done.run(pong);
  }
}