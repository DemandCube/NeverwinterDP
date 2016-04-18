package com.neverwinterdp.netty.rpc.server;

import java.net.InetAddress;
import java.net.UnknownHostException;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.neverwinterdp.netty.rpc.protocol.WirePayload;

public final class RPCServer {
  static final boolean SSL  = System.getProperty("ssl") != null;
  static final int     PORT = Integer.parseInt(System.getProperty("port", "8463"));
  
  private Logger logger ;
  private EventLoopGroup bossGroup ;
  private EventLoopGroup workerGroup ;
  private int port  = PORT ;
  private DeamonThread deamonThread ;
  private ServiceRegistry serviceRegistry ;
  
  public RPCServer() {
    this.logger = LoggerFactory.getLogger(RPCServer.class.getSimpleName()) ;
    this.serviceRegistry = new ServiceRegistry() ;
  }
  
  public RPCServer(int port) {
    this() ;
    this.port = port ;
  }

  public ServiceRegistry getServiceRegistry() { return this.serviceRegistry ; }
  
  public String getHostIpAddress() throws UnknownHostException {
    return InetAddress.getLocalHost().getHostAddress()  ;
  }
  
  public int getPort() { return this.port ; }
  
  public RPCServer setPort(int port) { 
    this.port = port ;
    return this ;
  }

  public RPCServer setLogger(LoggerFactory factory) { 
    this.logger = factory.getLogger(getClass()) ;
    return this ;
  }
  
  public void start() throws Exception {
    logger.info("start start()");

    bossGroup = new NioEventLoopGroup(1);
    workerGroup = new NioEventLoopGroup();
    ServerBootstrap b = new ServerBootstrap();
    b.group(bossGroup, workerGroup).
      channel(NioServerSocketChannel.class).
      handler(new LoggingHandler(LogLevel.INFO)).
      childHandler(new ChannelInitializer<SocketChannel>() {
        public void initChannel(SocketChannel ch) throws Exception {
          ChannelPipeline p = ch.pipeline();
          p.addLast(new ProtobufVarint32FrameDecoder());
          p.addLast(new ProtobufDecoder(WirePayload.getDefaultInstance()));

          p.addLast(new ProtobufVarint32LengthFieldPrepender());
          p.addLast(new ProtobufEncoder());

          p.addLast(new RPCServerHandler(serviceRegistry));
        }
      });
    logger.info("finish start()");
    logger.info("bind and waiting for request");
    b.bind(port).sync().channel().closeFuture().sync();
  }
  
  public void startAsDeamon() {
    deamonThread = new DeamonThread(this) ;
    deamonThread.start() ; 
  }
  
  public void shutdown() {
    logger.info("start shutdown()");
    if(bossGroup != null) bossGroup.shutdownGracefully();
    if(workerGroup != null) workerGroup.shutdownGracefully();
    bossGroup = null ;
    workerGroup = null ;
    logger.info("finish shutdown()");
  }
  
  static public class DeamonThread extends Thread {
    RPCServer instance ;
    
    DeamonThread(RPCServer instance) {
      this.instance = instance ;
    }
    
    public void run() {
      try {
        instance.start();
      } catch (InterruptedException e) {
      } catch (Exception e) {
        instance.logger.error("RPCServer deamon thread has problem.", e);
      } finally {
        instance.logger.info("Exit deamon thread") ;
      }
    }
  }
}
