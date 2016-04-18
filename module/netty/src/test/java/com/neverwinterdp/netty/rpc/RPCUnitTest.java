package com.neverwinterdp.netty.rpc;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.google.protobuf.ByteString;
import com.neverwinterdp.netty.rpc.protocol.Request;
import com.neverwinterdp.netty.rpc.protocol.Response;
import com.neverwinterdp.netty.rpc.client.RPCClient;
import com.neverwinterdp.netty.rpc.server.RPCServer;

public class RPCUnitTest {
  static protected RPCServer server  ;
  static protected RPCClient client ;
  
  @BeforeClass
  static public void setup() throws Exception {
    server = new RPCServer() ;
    server.startAsDeamon(); 
    
    client = new RPCClient() ;
    
//    Request.Builder requestB = Request.newBuilder();
//    requestB.setServiceId("AService") ;
//    requestB.setMethodId("AMethod") ;
//    requestB.setParams(ByteString.EMPTY) ;
//    Response response = client.call(requestB.build()) ;
//    System.out.format("Server RPCMessage = " + new String(response.getResult().toByteArray()));
  }
  
  @AfterClass
  static public void teardown() {
    client.close();
    server.shutdown();
  }
}
