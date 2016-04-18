package com.neverwinterdp.netty.rpc;

import org.junit.Test;

import com.google.protobuf.RpcCallback;
import com.neverwinterdp.netty.rpc.client.DefaultClientRPCController;
import com.neverwinterdp.netty.rpc.ping.PingServiceImpl;
import com.neverwinterdp.netty.rpc.ping.protocol.Ping;
import com.neverwinterdp.netty.rpc.ping.protocol.PingService;
import com.neverwinterdp.netty.rpc.ping.protocol.Pong;

public class RpcChannelClientUnitTest extends RPCUnitTest {
  
  @Test
  public void testBlockingService() throws Exception {
    server.getServiceRegistry().register(PingService.newReflectiveBlockingService(new PingServiceImpl()));

    PingService.BlockingInterface blockingService = PingService.newBlockingStub(client.getRPCChannel()) ;
    Ping.Builder pingB = Ping.newBuilder();
    pingB.setMessage("Hello Ping") ;
    Pong pong = blockingService.ping(new DefaultClientRPCController(), pingB.build());
    System.out.println("Pong = " + pong);
    server.getServiceRegistry().remove(PingService.getDescriptor().getFullName()) ;
  }
  
  @Test
  public void testNonBlockingService() throws Exception {
    server.getServiceRegistry().register(PingService.newReflectiveService(new PingServiceImpl()));
    
    PingService.Interface nonBlockingService = PingService.newStub(client.getRPCChannel()) ;
    RpcCallback<Pong> done = new RpcCallback<Pong>() {
      public void run(Pong pong) {
        System.out.println("RpcCallback<Pong>: " + pong);
      }
    };
    Ping.Builder pingB = Ping.newBuilder();
    pingB.setMessage("Hello Ping") ;
    nonBlockingService.ping(new DefaultClientRPCController(), pingB.build(), done);
    System.out.println("wait.....");
    Thread.sleep(2000);
    server.getServiceRegistry().remove(PingService.getDescriptor().getFullName()) ;
  }
}
