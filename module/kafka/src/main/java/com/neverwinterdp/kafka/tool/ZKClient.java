package com.neverwinterdp.kafka.tool;

import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.neverwinterdp.util.JSONSerializer;

public class ZKClient {
  private String zkConnects;
  private ZooKeeper zkClient ;
  
  private boolean closed = false;
  
  public ZKClient() {
  }
  
  public ZKClient(String zkConnects) {
    this.zkConnects = zkConnects;
  }
  
  public void connect(long timeout) throws Exception {
    reconnect(timeout) ;
  }
  
  synchronized void reconnect(long timeout) throws Exception {
    if(closed) {
      throw new Exception("Zk connection has been closed");
    }
    if(zkClient != null) zkClient.close();
    Watcher watcher = new Watcher() {
      @Override
      public void process(WatchedEvent event) {
      }
    };
    zkClient = new ZooKeeper(zkConnects, 15000, watcher);
    long waitTime = 0 ;
    while(waitTime < timeout) {
      if(zkClient.getState().isConnected()) {
        return ;
      }
      Thread.sleep(500);
      waitTime += 500;
    }
    zkClient.close();
    zkClient = null ;
    throw new Exception("Cannot connect after " + timeout + "ms") ;
  }

  synchronized public void close() throws Exception {
    if(closed) return;
    closed = true;
    if(zkClient != null) {
      zkClient.close();;
      zkClient = null ;
    }
  }

  synchronized ZooKeeper getZKClient() throws Exception {
    if(closed) {
      throw new Exception("Zk connection  has been closed");
    }
    return zkClient;
  }
  
  public boolean isConnect() { 
    return zkClient != null  && zkClient.getState().isConnected() ;
  }
  
  public void dump(final String path) throws Exception {
    Operation<Boolean> op = new Operation<Boolean>() {
      @Override
      public Boolean execute(ZooKeeper zkClient) throws InterruptedException, KeeperException {
        dump(zkClient, path);
        return true;
      }
    };
    execute(op, 1);
  }
  
  public <T> T execute(Operation<T> op, int retry) throws Exception {
    Exception error = null;
    for (int i = 0; i < retry; i++) {
      try {
        ZooKeeper zkClient = getZKClient();
        T result = op.execute(zkClient);
        return result;
      } catch (KeeperException kEx) {
        reconnect(10000);
        error = kEx;
      }
    }
    throw error;
  }
  
  static public abstract class Operation<T> {
    abstract public T execute(ZooKeeper zkClient) throws InterruptedException, KeeperException ;
 
    protected <V> V getDataAs(ZooKeeper zkClient, String path, Class<V> type) throws KeeperException, InterruptedException {
      byte[] data = zkClient.getData(path, false, new Stat());
      return JSONSerializer.INSTANCE.fromBytes(data, type);
    }
    
    protected void dump(ZooKeeper zkClient, String path) throws KeeperException, InterruptedException {
      System.out.println(path) ;
      List<String> children = zkClient.getChildren(path, false);
      for(int i = 0; i < children.size(); i++) {
        dump(zkClient, path, children.get(i), "  ");
      }
    }
    
    protected void dump(ZooKeeper zkClient, String parentPath, String node, String indentation) throws KeeperException, InterruptedException {
      String path = parentPath + "/" + node;
      byte[] bytes = zkClient.getData(path, false, new Stat());
      if(bytes != null) {
        String data = new String(bytes);
        if(data.length() > 120 ) data = data.substring(0, 120);
        System.out.println(indentation + node + " - " + data) ;
      } else {
        System.out.println(indentation + node) ;
      }
      List<String> children = zkClient.getChildren(path, false);
      String childIndentation = indentation + "  " ;
      for(int i = 0; i < children.size(); i++) {
        dump(zkClient, path, children.get(i), childIndentation);
      }
    }
  }
}