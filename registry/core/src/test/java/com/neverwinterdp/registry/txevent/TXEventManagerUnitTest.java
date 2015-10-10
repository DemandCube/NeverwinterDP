package com.neverwinterdp.registry.txevent;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.zk.tool.server.EmbededZKServer;

public class TXEventManagerUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties") ;
  }
  
  static public String TX_EVENTS_PATH = "/tx-events";
  
  static private EmbededZKServer zkServerLauncher ;
  
  @BeforeClass
  static public void startServer() throws Exception {
    FileUtil.removeIfExist("./build/data", false);
    zkServerLauncher = new EmbededZKServer("./build/data/zookeeper") ;
    zkServerLauncher.start();
  }
  
  static public void stopServer() throws Exception {
    zkServerLauncher.shutdown();
  }
  
  private Registry registry ;
  
  @Before
  public void setup() throws Exception {
    registry = RegistryConfig.getDefault().newInstance();
    registry.connect();
    registry.createIfNotExist(TX_EVENTS_PATH);
  }
  
  @After
  public void teardown() throws Exception {
    registry.rdelete(TX_EVENTS_PATH);
    registry.shutdown();
  }

  @Test
  public void test() throws Exception {
    TXEventBroadcaster manager = new TXEventBroadcaster(registry, TX_EVENTS_PATH);
    
    TXEventWorker worker1 = new TXEventWorker("worker-1") ;
    worker1.start();
    
    TXEventWorker worker2 = new TXEventWorker("worker-2") ;
    worker2.start();
    
    Thread.sleep(1000);
    
    TXEventNotificationListener listener = new TXEventNotificationListener() {
      @Override
      public void onNotification(TXEvent event, TXEventNotification notification) throws Exception {
        System.out.println("Got a notification from the client " + notification.getClientId()) ;
      }
    };
    TXEvent txEvent = new TXEvent("test", System.currentTimeMillis() + 30000, new byte[0]);
    TXEventNotificationWatcher watcher = manager.broadcast(txEvent, listener);
    watcher.waitForNotifications(2, 5000);
    watcher.complete();
    worker1.interrupt();
    worker2.interrupt();
    System.err.println("Done!!!");
  }
  
  static public class TXEventWorker extends Thread {
    private String workerId ;
    
    public TXEventWorker(String workerId) {
      this.workerId = workerId ;
    }
    
    public void run() {
      try {
        execute() ;
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    
    public void execute() throws Exception {
      Registry registry = RegistryConfig.getDefault().newInstance();
      registry.connect();
      
      TXEventWatcher watcher = new TXEventWatcher(registry, TX_EVENTS_PATH, workerId) {
        public void onTXEvent(TXEvent txEvent) throws Exception {
          System.out.println("worker " + workerId + " got tx event " + txEvent.getId()) ;
          super.onTXEvent(txEvent);
        }
      };
      System.out.println(workerId + "wait to exit");
      try {
        Thread.sleep(30000);
      } catch (InterruptedException e) {
      }
      watcher.setComplete();
    }
  }
}