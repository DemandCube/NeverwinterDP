package com.neverwinterdp.netty.multicast;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;


/**
 * 
 * This class is a UDP server.  This server listens on the specified port you configure it,
 * and will listen to broadcast messages and respond accordingly.
 * 
 * ****************************************************
 * 
 * Simple server to print out a message over UDP over a configured port
 * 
 * Example 1: Broadcasting a single message:
 *   This will open up a server on port 1111
 *   Any time somebody sends your server a UDP message on that port, it will respond with "Neverwinter Rocks!"
 *     MulticastServer broadcaster = new MulticastServer(1111, "Neverwinter Rocks!");
 *     broadcaster.run();
 *     broadcaster.stop();
 * 
 * 
 * ****************************************************
 * 
 * Example 2: Sending a map of messages over UDP:
 * 
 *   This will open up a server on port 111
 * 
 *   Any time somebody sends your server a UDP message on that port, 
 *   it will read in the message and determine if that message is a key in the map
 *   If it is a key in the map, it will return the associated value
 *   
 * Otherwise it will return the string "ERROR"
 * 
 *   In this specific example, if you sent a UDP message to the server on port 1111 
 *   with the string "dev" in the payload,
 *   It would return the string "1.1.1.1:8080"
 *   
 *     Map<String, String> m = new HashMap<String, String>();
 *     m.put("dev", "1.1.1.1:8080");
 *     m.put("local", "2.2.2.2:1111");
 *     m.put("prod","3.3.3.3:1234,3.3.3.4:1234,3.3.3.3:1234");
 *     MulticastServer broadcaster = new MulticastServer(1111, m);
 *     broadcaster.run();
 *     broadcaster.stop(); 
 * 
 * *****************************************************
 * 
 * To test your server, you can use netcat:
 * #> nc -u [hostname/localhost] [port number]
 * 
 * *****************************************************
 * 
 *  //To programmatically test your server, you can use UDPClient
 *  int udpport = 1234;
 *  UDPClient x = new UDPClient("localhost",udpport);
 *  String received = x.sendMessage("Hi UDP Server!");
 *  
 *  //Alternatively, to send a broadcast to your local subnet, use 255 as the last 3 digits of the IP -
 *  int udpport = 1234; 
 *  UDPClient x = new UDPClient("192.168.1.255",udpport);
 *  String received = x.sendMessage("Hi UDP Server!");
 * 
 * ****************************************************
 * 
 * @author Richard Duarte
 *
 */
public class MulticastServer{
  private int port;
  private String message="";
  Map<String, String> messageMap = new HashMap<String, String>();
  private Logger logger;
  private EventLoopGroup group=null;
  
  
  /**
   * Constructor to broadcast a single, simple string
   * @param port Port to run on
   * @param msg Message to broadcast
   */
  public MulticastServer(int Port, String msg){
    this.port = Port;
    this.message = msg;
    this.logger = LoggerFactory.getLogger(getClass().getSimpleName()) ;
    logger.info("MulticastServer initialized.  Response string is: "+msg);
  }
  
  /**
   * Constructor to use message map
   * @param port Port to run on 
   * @param msg Map of <String,String> Where format is <Request String, Response String>
   */
  public MulticastServer(int Port, Map<String, String> msg){
    this.port = Port;
    
    //Have to do a deep copy here
    this.messageMap = new HashMap<String,String>(msg);
    this.logger = LoggerFactory.getLogger(getClass().getSimpleName()) ;
    logger.info("MulticastServer initialized. Response hash is: "+msg.toString());
  }
  
  

  /**
   * Start the broadcasting UDP server
   */
  public void run(){
    this.group = new NioEventLoopGroup();
    Bootstrap b = new Bootstrap();
    ChannelInitializer<Channel> ch = null;
    
    //Create channel pipeline
    //Use message map
    if(this.message.isEmpty()){
      ch = new ChannelInitializer<Channel>() {
         @Override
         protected void initChannel (Channel channel) throws Exception {
          ChannelPipeline pipeline = channel.pipeline();
          pipeline.addLast("decoder",new MulticastServerDecoder());
          pipeline.addLast("encoder",new MulticastServerHandler(messageMap));
        }
       };
    }
    //Use simple single message
    else{
      ch = new ChannelInitializer<Channel>() {
        @Override
        protected void initChannel (Channel channel) throws Exception {  
         ChannelPipeline pipeline = channel.pipeline();
         pipeline.addLast("decoder", new MulticastServerDecoder());
         pipeline.addLast("encoder", new MulticastServerHandler(message));
       }
      };
    }
    
    b.group(group)
    .channel(NioDatagramChannel.class)
    .option(ChannelOption.SO_BROADCAST, true)
    .handler(ch);
    
    //Bind server
    b.bind(this.port).syncUninterruptibly().channel();
  }

  /**
   * Kill server gracefully
   */
  public void stop(){
    logger.info("Stopping server");
    this.group.shutdownGracefully();
  }
  
}
