package com.neverwinterdp.registry.zk;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;

public class ZkConnectedStateWatcher implements Watcher {
  private boolean connected = false;
  
  public void process(WatchedEvent event) {
    if(event.getState()  == KeeperState.SyncConnected) {
      notifyConnected();
    }
  }
  
  synchronized public void notifyConnected() {
    connected = true;
    notifyAll();
  }
  
  synchronized public boolean waitForConnected(long timeout) throws InterruptedException {
    wait(timeout);
    return connected;
  }
}