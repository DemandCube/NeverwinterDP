package com.neverwinterdp.netty.http;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.neverwinterdp.util.MapUtil;
/**
 * @author Tuan Nguyen
 * @email  tuan08@gmail.com
 */
public class HttpServer {
  private Logger  logger = LoggerFactory.getLogger(HttpServer.class.getSimpleName());
  private int     port = 8080;
  private int     numberOfWorkers = 3;
  private List<RouteHandler> handlers = new ArrayList<RouteHandler>() ;
  private RouteMatcher<RouteHandler> routeMatcher = new RouteMatcher<RouteHandler>() ;
  private Channel channel;
  EventLoopGroup bossGroup, workerGroup ;
  private Thread deamonThread ;
  
  public void configure(Map<String, String> props) throws Exception {
    port = MapUtil.getInteger(props, "port", 8080) ;
    String wwwDir = props.get("www-dir") ; 
    if(wwwDir != null) {
      StaticFileHandler staticFileHandler = new StaticFileHandler(wwwDir) ;
      staticFileHandler.configure(props);
      setDefault(staticFileHandler) ;
    }
    String[] routeNames = MapUtil.getStringArray(props, "route.names", new String[] {});
    for(String routeName : routeNames) {
      String prefix = "route." + routeName + ".";
      Map<String, String> routeHandlerProps = MapUtil.getSubMap(props, prefix) ;
      String handlerType = routeHandlerProps.get("handler") ;
      Class<RouteHandler> clazz = (Class<RouteHandler>) Class.forName(handlerType) ;
      RouteHandler handler = clazz.newInstance() ;
      handler.configure(routeHandlerProps);
      String[] routePath   = MapUtil.getStringArray(routeHandlerProps, "path", null) ;
      add(routePath, handler) ;
    }
  }
  
  public String getHostIpAddress() throws UnknownHostException {
    return InetAddress.getLocalHost().getHostAddress()  ;
  }
  
  public int getPort() { return this.port; }
  public HttpServer setPort(int port) {
    this.port = port;
    return this ;
  }
  
  public int  getNumberOfWorkers() { return this.numberOfWorkers ; }
  public void setNumberOfWorkers(int workers) { this.numberOfWorkers = workers ; }
  
  public RouteMatcher<RouteHandler> getRouteMatcher() { return this.routeMatcher ; }

  public HttpServer add(String path, RouteHandler handler) {
    routeMatcher.addPattern(path, handler);
    handlers.add(handler) ;
    return this ;
  }
  
  public HttpServer add(String[] path, RouteHandler handler) {
    if(path == null) return this ;
    for(String sel : path) {
      routeMatcher.addPattern(sel, handler);
    }
    handlers.add(handler) ;
    return this ;
  }
  
  public HttpServer setDefault(RouteHandler handler) {
    routeMatcher.setDefaultHandler(handler);
    return this ;
  }
  
  public void start() throws Exception {
    logger.info("Start start()");
    if(routeMatcher.getDefaultHandler() == null) {
      setDefault(new NotFoundRouteHandler()) ;
    }
    bossGroup = new NioEventLoopGroup(1);
    workerGroup = new NioEventLoopGroup(numberOfWorkers);
    try {
      ServerBootstrap b = new ServerBootstrap();
      //b.childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, 32 * 1024);
      //b.childOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 8 * 1024);
      //b.childOption(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(16 * 1024 * 1024));
      ChannelInitializer<SocketChannel> initializer = new ChannelInitializer<SocketChannel>() {
        public void initChannel(SocketChannel ch) throws Exception {
          ChannelPipeline p = ch.pipeline();
          p.addLast("codec", new HttpServerCodec());
         
//          // Decodes ChannelBuffer into HTTP Request message
//          p.addLast("decoder", new HttpRequestDecoder());
//          // Encodes HTTTPRequest message to ChannelBuffer
//          p.addLast("encoder", new HttpResponseEncoder());
         
          //Remove the following line if you don't want automatic content compression.
          //p.addLast("deflater", new HttpContentCompressor());
          //handle automatic content decompression.
          //p.addLast("inflater", new HttpContentDecompressor());
          
          p.addLast("aggregator", new HttpObjectAggregator(3 * 1024 * 1024));
          p.addLast("handler", new HttpServerHandler(HttpServer.this));
        }
      };
      b.option(ChannelOption.SO_BACKLOG, 1024);
      b.group(bossGroup, workerGroup).
        channel(NioServerSocketChannel.class).
        childHandler(initializer);
      channel = b.bind(port).sync().channel();
      InetSocketAddress addr = (InetSocketAddress)channel.localAddress() ;
      this.port = addr.getPort() ;
      logger.info("bind port successfully, channel = " + channel);
      logger.info("Start start() waitting to handle request");
      channel.closeFuture().sync();
    } finally {
      bossGroup.shutdownGracefully();
      workerGroup.shutdownGracefully();
    }
  }
  
  public void startAsDeamon() {
    deamonThread = new DeamonThread(this) ;
    deamonThread.start() ; 
  }

  public void shutdown() {
    logger.info("Start shutdown()");
    bossGroup.shutdownGracefully();
    workerGroup.shutdownGracefully();
    channel.close();
    for(RouteHandler handler : handlers) {
      handler.close();
    }
    logger.info("Finish shutdown()");
  }
  
  static public class DeamonThread extends Thread {
    HttpServer instance ;
    
    DeamonThread(HttpServer instance) {
      this.instance = instance ;
    }
    
    public void run() {
      try {
        instance.start();
      } catch (Exception e) {
        instance.logger.error("HttpServer deamon thread has problem.", e);
      }
    }
  }
}