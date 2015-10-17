package com.neverwinterdp.registry.zk;

import java.util.ArrayList;
import java.util.Collections;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.zk.tool.server.EmbededZKServer;

public class BugUnitTest {
  public final Id ANYONE_ID = new Id("world", "anyone");
  public final ArrayList<ACL> OPEN_ACL = new ArrayList<ACL>(Collections.singletonList(new ACL(Perms.ALL, ANYONE_ID)));
  
  private EmbededZKServer zkServerLauncher ;
  private ZooKeeper zkClient ;
  static String connection;

  @Before
  public void setup() throws Exception {
    FileUtil.removeIfExist("./build/data", false);
    
    zkServerLauncher = new EmbededZKServer("./build/data/zookeeper") ;
    zkServerLauncher.start();
    
    Watcher watcher = new Watcher() {
      public void process(WatchedEvent event) {
        System.out.println("on event: " + event.getPath() + " - " + event.getType() + " - " + event.getState());
      }
    };
    zkClient = new ZooKeeper("127.0.0.1:2181", 15000, watcher);
  }
  
  @After
  public void teardown() throws Exception {
    zkClient.close();
    zkServerLauncher.shutdown();
  }

  @Test
  public void testChroot() throws Exception  { 
    Watcher watcher = new Watcher()  { 
      @Override 
      public void process(WatchedEvent event) { 
        System.out.println("==> Event:" + event); 
      } 
    }; 
    ZooKeeper zk = new ZooKeeper("127.0.0.1:2181/foo", 6000, watcher); 
    //uncommenting this line will not cause infinite connect/disconnect 
    zk.exists("/test", true); 

    zk.create("/", new byte[0], OPEN_ACL, CreateMode.PERSISTENT);
    
    zk.create("/test", new byte[0], OPEN_ACL, CreateMode.PERSISTENT);
    
    zk.exists("/test", true); 
    
    zk.delete("/test", 0);
    System.out.println("Stop the server and restart it when you see this message"); 
    Thread.sleep(3000);
  } 

}