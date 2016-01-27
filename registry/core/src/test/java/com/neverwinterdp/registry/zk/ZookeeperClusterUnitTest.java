package com.neverwinterdp.registry.zk;

import java.io.IOException;
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
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.util.io.IOUtil;
import com.neverwinterdp.zookeeper.tool.server.EmbededZKServer;

public class ZookeeperClusterUnitTest {
  public final Id ANYONE_ID = new Id("world", "anyone");
  public final ArrayList<ACL> OPEN_ACL = new ArrayList<ACL>(Collections.singletonList(new ACL(Perms.ALL, ANYONE_ID)));
  
  private EmbededZKServer zkServerLauncher1 ;
  private EmbededZKServer zkServerLauncher2 ;
  private EmbededZKServer zkServerLauncher3 ;
  private String zkConnects ;
  private ZooKeeper zkClient ;

  @Before
  public void setup() throws Exception {
    FileUtil.removeIfExist("./build/data", false);
    
    FileUtil.mkdirs("./build/data/zookeeper-1");
    IOUtil.save("1", "UTF-8", "./build/data/zookeeper-1/myid");
    zkServerLauncher1 = new EmbededZKServer("./build/data/zookeeper-1", 2181) ;
    zkServerLauncher1.addEnsemble(1, "127.0.0.1:2888:3888");
    zkServerLauncher1.addEnsemble(2, "127.0.0.1:2999:3999");
    zkServerLauncher1.addEnsemble(3, "127.0.0.1:3000:4000");
    zkServerLauncher1.start();
    
    FileUtil.mkdirs("./build/data/zookeeper-2");
    IOUtil.save("2", "UTF-8", "./build/data/zookeeper-2/myid");
    zkServerLauncher2 = new EmbededZKServer("./build/data/zookeeper-2", 2182) ;
    zkServerLauncher2.addEnsemble(1, "127.0.0.1:2888:3888");
    zkServerLauncher2.addEnsemble(2, "127.0.0.1:2999:3999");
    zkServerLauncher2.addEnsemble(3, "127.0.0.1:3000:4000");
    zkServerLauncher2.start();
    
    FileUtil.mkdirs("./build/data/zookeeper-3");
    IOUtil.save("3", "UTF-8", "./build/data/zookeeper-3/myid");
    zkServerLauncher3 = new EmbededZKServer("./build/data/zookeeper-3", 2183) ;
    zkServerLauncher3.addEnsemble(1, "127.0.0.1:2888:3888");
    zkServerLauncher3.addEnsemble(2, "127.0.0.1:2999:3999");
    zkServerLauncher3.addEnsemble(3, "127.0.0.1:3000:4000");
    zkServerLauncher3.start();
    zkConnects = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";
    
    zkClient = newZkClient();
  }
  
  @After
  public void teardown() throws Exception {
    zkClient.close();
    zkServerLauncher1.shutdown();
    zkServerLauncher2.shutdown();
    zkServerLauncher3.shutdown();
  }

  @Test
  public void testZkClient() throws Exception {
    System.err.println(zkClient.toString());
    System.err.println("zkClient session id = " + zkClient.getSessionId());
    zkClient.create("/path1", "path1".getBytes(), OPEN_ACL, CreateMode.EPHEMERAL) ;
    System.err.println("before shutdown");
    zkServerLauncher2.shutdown();
    System.err.println("after shutdown");
    for(int i = 1; i <= 10; i++) {
      try {
        zkClient.create("/path2", "path2".getBytes(), OPEN_ACL, CreateMode.PERSISTENT) ;
        break;
      } catch(Exception ex) {
        System.err.println(i + ". Cannot create due to " + ex.getMessage() + ", state = " + zkClient.getState());
        Thread.sleep(1000);
      }
    }
    Thread.sleep(5000);
    byte[] path1Data = zkClient.getData("/path1", false, new Stat());
    System.err.println("path1Data = " + new String(path1Data));
    zkClient.close();
  }
  
  ZooKeeper newZkClient() throws IOException, InterruptedException {
    Watcher watcher = new Watcher() {
      public void process(WatchedEvent event) {
        System.out.println("on event: path = " + event.getPath() + ", type =  " + event.getType() + ", state = " + event.getState());
      }
    };
    zkClient = new ZooKeeper(zkConnects, 3000, watcher);
    int count = 0;
    while(!zkClient.getState().isConnected()) {
      System.err.println(count++ + ". state = " + zkClient.getState());
      Thread.sleep(1000);
    }
    return zkClient;
  }
}