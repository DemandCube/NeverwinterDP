package com.neverwinterdp.registry.txevent;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.event.NodeChildrenWatcher;
import com.neverwinterdp.registry.event.NodeEvent;

public class TXEventNotificationWatcher extends NodeChildrenWatcher {
  private TXEvent                          txEvent; 
  private TXEventNotificationListener      listener ;
  private Map<String, TXEventNotification> processedNotifications = new HashMap<>();
  
  public TXEventNotificationWatcher(TXEventBroadcaster manager, TXEvent event, TXEventNotificationListener listener) throws RegistryException {
    super(manager.getRegistry(), true);
    txEvent = event;
    this.listener = listener ;
    watchChildren(manager.getEventPath() + "/" + event.getId());
  }

  public Map<String, TXEventNotification> getProcessedNotifcations() {
    return this.processedNotifications; 
  }
  
  synchronized public void checkAndProcessNotifications() throws Exception {
    Registry registry = getRegistry();
    String eventPath = getWatchedPath() ;
    List<String> names = registry.getChildren(eventPath);
    int processCount = 0;
    for(String name : names) {
      if(processedNotifications.containsKey(name)) continue ;
      TXEventNotification notification = registry.getDataAs(eventPath + "/" + name, TXEventNotification.class); 
      listener.onNotification(txEvent, notification);
      processedNotifications.put(name, notification);
      processCount++ ;
    }
    if(processCount > 0) {
      notify();
    }
  }
  
  public int waitForNotifications(int expectNumOfNotification, long timeout) throws Exception {
    long stopTime = System.currentTimeMillis() + timeout;
    long waitTime = timeout ;
    while(processedNotifications.size() < expectNumOfNotification) {
      waitTime = stopTime - System.currentTimeMillis();
      if(waitTime <= 0) break;
      waitForNotification(waitTime);
    }
    boolean complete = processedNotifications.size() == expectNumOfNotification;
    if(!complete) {
      checkAndProcessNotifications();
    }
    return processedNotifications.size();
  }

  synchronized int waitForNotification(long timeout) throws InterruptedException {
    wait(timeout);
    return processedNotifications.size();
  }
  
  public void complete() throws RegistryException {
    setComplete();
    getRegistry().rdelete(getWatchedPath());
  }
  
  @Override
  public void processNodeEvent(NodeEvent event) throws Exception {
    if(event.getType() == NodeEvent.Type.CHILDREN_CHANGED) {
      checkAndProcessNotifications();
    }
  }
}
