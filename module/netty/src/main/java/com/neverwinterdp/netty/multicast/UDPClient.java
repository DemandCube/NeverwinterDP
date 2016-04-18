package com.neverwinterdp.netty.multicast;


import io.netty.util.CharsetUtil;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

/**
 * Super simple UDP Client to send UDP packets to a server and return the response
 * Currently just being used for testing
 * 
 * Example usage:
 *
 * UDPClient x = new UDPClient("localhost",1111);
 * //Sends UDP packet "Hi UDP Server!" and prints out the string of what gets returned from Server 
 * System.out.println("Received: "+x.sendMessage("Hi UDP Server!"));
 * 
 * To send a broadcast on your local subnet:
 * //Sends UDP broadcast to IP's 192.168.1.1 to 192.168.1.254
 * UDPClient x = new UDPClient("192.168.1.255",1111); 
 * System.out.println("Received: "+x.sendMessage("Hi UDP Server!"));
 * 
 * @author Richard Duarte
 */
public final class UDPClient {

    private int port;
    private String host;
    
    public UDPClient(String Host, int Port){
      this.host = Host;
      this.port = Port;
    }
    
    /**
     * Send a message via UDP.  Sets responseSize to automagic 1024
     * @param message The message to send over the wire
     * @return String response from server
     * @throws InterruptedException
     */
    public String sendMessage(String message) throws InterruptedException{
      return this.sendMessage(message,1024);
    }
    
  /**
   * Send a message via UDP
   * @param message The message to send over the wire
   * @param responseSize The size of the byte[] to store the response
   * @return String response from server
   * @throws InterruptedException
   */
  public String sendMessage(String message,int responseSize) throws InterruptedException{
      String retVal=null;
      try {
        byte[] msg = message.getBytes( CharsetUtil.UTF_8);
        // Get the internet address of the specified host
        InetAddress address = InetAddress.getByName(this.host);

        // Initialize a datagram packet with data and address
        DatagramPacket sendPacket = new DatagramPacket(msg, msg.length, address, port);

        // Create a datagram socket, send the packet through it
        DatagramSocket dsocket = new DatagramSocket();
        dsocket.send(sendPacket);
        
        //If we can't allocate memory for the response
        //Then just return
        if(responseSize < 1){
          dsocket.close();
          return "";
        }
        
        //Get response
        byte[] buf = new byte[responseSize];
        DatagramPacket receivePacket = new DatagramPacket(buf, buf.length);
        dsocket.receive(receivePacket);
        
        retVal = new String(receivePacket.getData(), 0, receivePacket.getLength());
        
        dsocket.close();
      } 
      catch (Exception e) {
        System.err.println(e);
      }
      return retVal;
    }

}
