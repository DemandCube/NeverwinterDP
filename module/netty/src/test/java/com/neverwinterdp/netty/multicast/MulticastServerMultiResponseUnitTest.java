package com.neverwinterdp.netty.multicast;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.netty.multicast.MulticastServer;
import com.neverwinterdp.netty.multicast.UDPClient;

/**
 * Tests to ensure the multi response functionality of Multicast server works
 * @author Richard Duarte
 *
 */
public class MulticastServerMultiResponseUnitTest {
  private MulticastServer server ;
  Map<String, String> m = new HashMap<String, String>();
  private int udpport = 54555;
  
  
  /**
   * Create server, pass in hash map argument
   */
  @Before
  public void setup() throws Exception {
    m.put("dev", "1.1.1.1:8080");
    m.put("local", "2.2.2.2:1111");
    m.put("prod","3.3.3.3:1234,3.3.3.4:1234,3.3.3.3:1234");
    server = new MulticastServer(udpport, m);
    server.run() ;
    Thread.sleep(1000);
  }
    
  /**
   * Stop server
   */
    @After
    public void teardown() {
      try{
        server.stop();
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
    
    /**
     * Test that each key in the hash map returns the correct value and that an
     * invalid key returns "ERROR"
     * @throws Exception
     */
    @Test(timeout=60000)
    public void testMultiResponseBroadcastServer() throws Exception {
      UDPClient x = new UDPClient("localhost",udpport); 
      for (Map.Entry<String, String> entry : m.entrySet()) {
        String key = entry.getKey();
        String value = entry.getValue();
        String received = x.sendMessage(key);
        assertEquals(value, received);
      }
      String received = x.sendMessage("Force an error!");
      assertEquals("ERROR", received);
    }
    
    /**
     * Test that each key in the hash map returns the correct value and that an
     * invalid key returns "ERROR"
     * Run this 100 times
     * @throws Exception
     */
    @Test(timeout=60000)
    public void testMultiResponseBroadcastServer100Times() throws Exception {
      UDPClient x = new UDPClient("localhost",udpport); 
      for(int i=0; i<100; i++){
        for (Map.Entry<String, String> entry : m.entrySet()) {
          String key = entry.getKey();
          String value = entry.getValue();
          String received = x.sendMessage(key);
          assertEquals(value, received);
        }
        String received = x.sendMessage("Force an error!");
        assertEquals("ERROR", received);
      }
    }

}
