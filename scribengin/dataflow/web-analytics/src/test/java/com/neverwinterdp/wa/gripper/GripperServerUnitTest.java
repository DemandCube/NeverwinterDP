package com.neverwinterdp.wa.gripper;

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.netty.http.client.AsyncHttpClient;
import com.neverwinterdp.netty.http.client.DumpResponseHandler;
import com.neverwinterdp.scribengin.LocalScribenginCluster;
import com.neverwinterdp.scribengin.shell.ScribenginShell;
import com.neverwinterdp.wa.event.BrowserInfo;
import com.neverwinterdp.wa.event.WebEvent;
import com.neverwinterdp.wa.event.generator.Browsers;
import com.neverwinterdp.wa.event.generator.WebEventGenerator;
/**
 * @author Tuan Nguyen
 * @email  tuan08@gmail.com
 */
public class GripperServerUnitTest {
  GripperServer          server;
  LocalScribenginCluster localScribenginCluster;
  ScribenginShell        shell;

  @Before
  public void setup() throws Exception {
    String BASE_DIR = "build/working";
    System.setProperty("app.home",   BASE_DIR + "/scribengin");
    System.setProperty("vm.app.dir", BASE_DIR + "/scribengin");
    
    localScribenginCluster = new LocalScribenginCluster(BASE_DIR) ;
    localScribenginCluster.clean(); 
    localScribenginCluster.useLog4jConfig("classpath:scribengin/log4j/vm-log4j.properties");  
    localScribenginCluster.start();
    
    shell = localScribenginCluster.getShell();
    
    server = new GripperServer();
    server.start();
  }
  
  @After
  public void teardown() throws Exception {
    server.shutdown();
    localScribenginCluster.shutdown();
  }
  
  @Test
  public void test() throws Exception {
    DumpResponseHandler handler = new DumpResponseHandler() ;
    AsyncHttpClient client = new AsyncHttpClient ("127.0.0.1", 8080, handler) ;
    WebEventGenerator webEventGenerator = new WebEventGenerator();
    BrowserInfo browser = Browsers.createMacbookAirBrowser();
    for(int i = 0; i < 1000; i++) {
      WebEvent wEvent = webEventGenerator.next(browser, "user.click", "GET", "http://google.com");
      client.post("/webevent/user.click", wEvent);
    }
    Thread.sleep(3000);
    assertEquals(1000, handler.getCount()) ;
  }
}