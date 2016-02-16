package com.neverwinterdp.netty.multicast;

import java.net.InetSocketAddress;

/**
 * Simple class to store necessary data for messages in multicast server
 * @author Richard Duarte
 *
 */
public class messageObject {
  public String message;
  public InetSocketAddress address;
  
  public messageObject(String msg,InetSocketAddress addy){
    this.message = msg;
    this.address = addy;
  }
}
