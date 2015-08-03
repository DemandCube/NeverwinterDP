package com.neverwinterdp.registry.txevent;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.event.NodeChildrenWatcher;
import com.neverwinterdp.registry.event.NodeEvent;

public class TXEventWatcher extends NodeChildrenWatcher {
  private String               clientId;
  private Node                 eventsNode;
  private Map<String, TXEvent> processedTxEvents = new HashMap<>();

  public TXEventWatcher(Registry registry, String eventsPath, String clientId) throws RegistryException {
    super(registry, true);
    this.clientId = clientId ;
    eventsNode = registry.get(eventsPath) ;
    watchChildren(eventsPath);
  }
  
  public String getClientId() { return this.clientId; }
  
  public void notify(TXEvent event, TXEventNotification notification) throws RegistryException {
    Node eventNode = eventsNode.getChild(event.getId());
    eventNode.createChild("notitification-", notification, NodeCreateMode.PERSISTENT_SEQUENTIAL);
  }

  public void notify(TXEvent event, TXEventNotification.Status status) throws RegistryException {
    TXEventNotification notification = new TXEventNotification(clientId, status) ;
    Node eventNode = eventsNode.getChild(event.getId());
    eventNode.createChild("notitification-", notification, NodeCreateMode.PERSISTENT_SEQUENTIAL);
  }
  
  synchronized public void checkAndProcessTXEvents() throws Exception {
    Registry registry = getRegistry();
    String eventsPath = getWatchedPath() ;
    List<String> names = registry.getChildren(eventsPath);
    for(String name : names) {
      if(processedTxEvents.containsKey(name)) continue ;
      TXEvent txEvent = registry.getDataAs(eventsPath + "/" + name, TXEvent.class); 
      onTXEvent(txEvent);
      processedTxEvents.put(name, txEvent);
    }
  }
  
  @Override
  public void processNodeEvent(NodeEvent event) throws Exception {
    if(event.getType() == NodeEvent.Type.CHILDREN_CHANGED) {
      checkAndProcessTXEvents();
    }
  }
  
  public void onTXEvent(TXEvent txEvent) throws Exception {
    notify(txEvent, TXEventNotification.Status.Complete);
  }
}