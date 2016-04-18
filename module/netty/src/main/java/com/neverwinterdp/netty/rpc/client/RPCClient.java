/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.neverwinterdp.netty.rpc.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.neverwinterdp.netty.rpc.protocol.Request;
import com.neverwinterdp.netty.rpc.protocol.Response;
import com.neverwinterdp.netty.rpc.server.RPCServer;

/**
 * Sends a list of continent/city pairs to a {@link RPCServer} to get the
 * local times of the specified cities.
 */
public final class RPCClient {
  static final boolean      SSL    = System.getProperty("ssl") != null;
  static final String       HOST   = System.getProperty("host", "127.0.0.1");
  static final int          PORT   = Integer.parseInt(System.getProperty("port", "8463"));

  private Channel channel ;
  private RPCChannelClient rpcChannel ;
  private EventLoopGroup eventLoopGroup ;
  private RPCClientHandler handler ;
 
  public RPCClient() throws Exception {
    this("127.0.0.1", 8463) ;
  }
  
  public RPCClient(String host, int port) throws Exception {
    SslContext sslCtx = null ;
    if(SSL) {
      sslCtx = SslContext.newClientContext(InsecureTrustManagerFactory.INSTANCE);
    } 

    eventLoopGroup = new NioEventLoopGroup();
    Bootstrap b = new Bootstrap();
    b.group(eventLoopGroup)
    .channel(NioSocketChannel.class)
    .handler(new RPCClientInitializer(sslCtx));

    // Make a new connection.
    channel = b.connect(host, port).sync().channel();
    // Get the handler instance to initiate the request.
    handler = channel.pipeline().get(RPCClientHandler.class);
    this.rpcChannel = new RPCChannelClient(this) ;
  }
  
  public void call(Request request, WirePayloadCallback callback) {
    handler.call(request, callback);
  }
  
  public Response call(Request request) throws Exception {
    Response response = handler.call(request);
    return response ;
  }
  
  public RPCChannelClient getRPCChannel() { return this.rpcChannel ; }
  
  public void close() {
    channel.close();
    eventLoopGroup.shutdownGracefully();
  }
}
