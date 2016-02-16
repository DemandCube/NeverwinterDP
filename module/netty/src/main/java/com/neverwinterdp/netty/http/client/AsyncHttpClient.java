package com.neverwinterdp.netty.http.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpVersion;

import java.net.ConnectException;
import java.net.URI;
import java.net.URISyntaxException;

import com.neverwinterdp.util.JSONSerializer;
/**
 * @author Tuan Nguyen
 * @email  tuan08@gmail.com
 */
public class AsyncHttpClient {
  private String host ;
  private int    port ;
  private Channel channel ;
  private EventLoopGroup group ;
  private ResponseHandler handler ;
  private boolean connected = false ;
  private JSONSerializer jsonSerializer = JSONSerializer.INSTANCE ;
  
  public AsyncHttpClient(String host, int port, ResponseHandler handler) throws Exception {
    this(host, port, handler, true) ;
  }
  
  public AsyncHttpClient(String host, int port, ResponseHandler handler, boolean connect) throws Exception {
    this.host = host ;
    this.port = port ;
    this.handler = handler ;
    if(connect) connect() ;
  }
  
  public void setJSONSerializer(JSONSerializer serializer) {
    jsonSerializer = serializer ;
  }
  
  public boolean connect() throws Exception {
    try {
      ChannelInitializer<SocketChannel> initializer = new ChannelInitializer<SocketChannel>() {
        public void initChannel(SocketChannel ch) throws Exception {
          ChannelPipeline p = ch.pipeline();
          //p.addLast("log", new LoggingHandler(LogLevel.INFO));
          p.addLast("codec", new HttpClientCodec());

          // Remove the following line if you don't want automatic content compression.
          //p.addLast("deflater", new HttpContentCompressor());
          //handle automatic content decompression.
          p.addLast("inflater", new HttpContentDecompressor());

          //handle HttpChunks.
          p.addLast("aggregator", new HttpObjectAggregator(3 * 1024 * 1024));
          p.addLast("handler", new HttpClientHandler(handler));
        }
      };

      group = new NioEventLoopGroup();
      Bootstrap b = new Bootstrap();
      //b.option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, 32 * 1024);
      //b.option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 8 * 1024);
      //b.option(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(16 * 1024 * 1024));
      b.group(group).
      channel(NioSocketChannel.class).
      handler(initializer);

      // Make the connection attempt.
      channel = b.connect(host, port).sync().channel();
      connected = true ;
      return true ;
    } catch(Exception ex) {
      connected = false;
      if(ex instanceof ConnectException) {
        return false ;
      }
      throw ex ;
    }
  }
  
  public boolean connect(long timeout, long tryPeriod) throws Exception {
    long stopTime = System.currentTimeMillis() + timeout ;
    while(System.currentTimeMillis() < stopTime) {
      Thread.sleep(tryPeriod);
      if(connect()) return true ;
    }
    return false ;
  }
  
  public boolean isConnected() { return connected ; }
  
  public void setNotConnected() { connected = false ; }
  
  public void close() {
    //Shut down executor threads to exit.
    if(channel != null) channel.close();
    if(group != null) group.shutdownGracefully();
  }
  
  public void await() throws InterruptedException {
    // Wait for the server to close the connection.
    channel.closeFuture().await() ;
  }
  
  public ChannelFuture get(String uriString) throws URISyntaxException, ConnectException {
    if(!connected) throw new ConnectException("Not Connected") ;
    URI uri = new URI(uriString);
    DefaultFullHttpRequest request = createRequest(uri, HttpMethod.GET, null) ;
    return channel.writeAndFlush(request) ;
  }
  
  public ChannelFuture post(String uriString, String data) throws ConnectException, URISyntaxException {
    ByteBuf content = Unpooled.wrappedBuffer(data.getBytes()) ;
    return post(uriString, content) ;
    //content.release() ;
  }
  
  public ChannelFuture post(String uriString, byte[] data) throws ConnectException, URISyntaxException {
    ByteBuf content = Unpooled.wrappedBuffer(data) ;
    return post(uriString, content) ;
    //content.release() ;
  }
  
  public <T> ChannelFuture post(String uriString, T object) throws ConnectException, URISyntaxException {
    byte[] data = jsonSerializer.toBytes(object) ;
    ByteBuf content = Unpooled.wrappedBuffer(data) ;
    return post(uriString, content) ;
    //content.release() ;
  }
  
  public ChannelFuture post(String uriString, ByteBuf content) throws ConnectException, URISyntaxException {
    if(!connected) throw new ConnectException("Not Connected") ;
    URI uri = new URI(uriString);
    DefaultFullHttpRequest request = createRequest(uri, HttpMethod.POST, content.retain()) ;
    return channel.writeAndFlush(request) ;
  }
  
  public void flush() { channel.flush() ; }
  
  DefaultFullHttpRequest createRequest(URI uri, HttpMethod method, ByteBuf content) {
    //Prepare the HTTP request.
    if(uri.getHost() != null && !host.equalsIgnoreCase(uri.getHost())) {
      throw new RuntimeException("expect uri with the host " + host) ;
    }
    if(uri.getPort() > 0 && port != uri.getPort()) {
      throw new RuntimeException("expect the port in uri = " + port) ;
    }
    
    DefaultFullHttpRequest request = null;
    if(content == null) { 
      request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, uri.toString());
    } else {
      request = 
          new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri.toString(), content);
      request.headers().set(HttpHeaders.Names.CONTENT_LENGTH, content.readableBytes());
      //HttpHeaders.setTransferEncodingChunked(request);
    }
    request.headers().set(HttpHeaders.Names.HOST, host);
    request.headers().set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
    request.headers().set(HttpHeaders.Names.ACCEPT_ENCODING, HttpHeaders.Values.GZIP);

    
    // Set some example cookies.
    //request.headers().set(
    //    HttpHeaders.Names.COOKIE,
    //    ClientCookieEncoder.encode(
    //        new DefaultCookie("my-cookie", "foo"),
    //        new DefaultCookie("another-cookie", "bar")
    //    )
    //);
    return request ;
  }
  
  
  static public class HttpClientHandler extends SimpleChannelInboundHandler<HttpObject> {
    private ResponseHandler responseHandler ;
    
    public HttpClientHandler(ResponseHandler handler) {
      this.responseHandler = handler ;
    }
    
    @Override
    public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
      if (!(msg instanceof HttpResponse)) return ;
      responseHandler.onResponse((HttpResponse) msg) ;
    }
  }
}