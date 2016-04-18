package com.neverwinterdp.netty.multicast;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.util.CharsetUtil;


/**
 * Decoder class for Multicast.  Turns datagram (udp) packet into messageObject
 * @author Richard Duarte
 * 
 */
public class MulticastServerDecoder extends MessageToMessageDecoder<DatagramPacket> {
  
  private Logger logger;
  
  /**
   * Constructor
   */
  public MulticastServerDecoder(){
    super();
    this.logger = LoggerFactory.getLogger(getClass().getSimpleName()) ;
      logger.info("MulticastServerDecoder initialized.");
  }
  
  
  /**
   * What to do if there's an exception.
   * We don't want to stop serving if there's an error
   */
  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    logger.info("Exception caught: "+cause.getMessage());
    // We don't close the channel because we can keep serving requests.
    cause.printStackTrace();
  }

  /**
   * Read in the data, pass it along to the next handler
   * Passes on a messageObject to the next handler
   */
  @Override
  protected void decode(ChannelHandlerContext ctx, DatagramPacket packet,List<Object> out) throws Exception {
    logger.info("Handling incoming message");
    String x = packet.content().toString(CharsetUtil.UTF_8).trim();
    out.add(new messageObject(x,packet.sender()));
  }
}
