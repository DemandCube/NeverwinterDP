package com.neverwinterdp.netty.multicast;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import io.netty.util.CharsetUtil;

/**
 * Handler to respond to our UDP messages
 * Takes in a messageObject and responds with the appropriate message
 * @author Richard Duarte
 *
 */
public class MulticastServerHandler extends SimpleChannelInboundHandler<messageObject>{
  private String message="";
  private Map<String,String> messageMap=null;
  private Logger logger;
  

  /**
   * Constructor
   * @param msg The single, simple message to broadcast
   */
  public MulticastServerHandler(String msg){
    this.message = msg;
    this.logger = LoggerFactory.getLogger(getClass().getSimpleName()) ;
      logger.info("MulticastServerHandler initialized.  Response string is: "+msg);
  }
  
  /**
   * Constructor
   * @param msg The message to broadcast. Format is <Request String, Response String>
   */
  public MulticastServerHandler(Map<String, String> msg){
    //Unfortunately, we have to do a copy here to make this assignment work
    this.messageMap = new HashMap<String,String>(msg);
    this.logger = LoggerFactory.getLogger(getClass().getSimpleName()) ;
    logger.info("MulticastServerHandler initialized.  Response map is: "+msg.toString());
  }
  
  /**
   * What to do if there's an exception.
   * We don't want to stop serving if there's an error
   */
  //@Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    logger.info("Exception caught: "+cause.getMessage());
    // We don't close the channel because we can keep serving requests.
    cause.printStackTrace();
  }
  
  /**
   * Here we take in the message and respond with the appropriate message
   * If we only have have message defined, then we respond with this.message
   * If we have messageMap defined, then see if msg.message exists in messageMap, and respond with the value
   */
  @Override
  public void channelRead0(ChannelHandlerContext ctx, messageObject msg) {
    logger.info("Handling incoming message");
    String toSend="";
    
    
    //If messageMap is null, then we only have the simple message to send
    if(null == this.messageMap){
      logger.info("Using simple message");
      toSend = this.message;
    }
    //Otherwise figure out if messageMap has the key that's been sent in
    else{
      //If the map contains that key, broadcast the value
      if(this.messageMap.containsKey(msg.message)){
        logger.info("Returning info for key:"+msg.message);
        toSend = this.messageMap.get(msg.message);
      }
      //Return ERROR if the key doesn't exist
      else{
        logger.info("key not found in message map: "+msg.message);
        toSend= "ERROR";
      }
    }
    
    logger.info("Packet payload is:"+toSend);
    ctx.writeAndFlush(new DatagramPacket(Unpooled.copiedBuffer(toSend, CharsetUtil.UTF_8), msg.address));
  }
  
  /**
   * Flush
   */
  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) {
      ctx.flush();
  }

  
}
