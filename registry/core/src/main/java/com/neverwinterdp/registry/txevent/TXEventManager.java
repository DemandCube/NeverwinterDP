package com.neverwinterdp.registry.txevent;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;

public class TXEventManager {
  private Registry         registry;
  private String           eventPath;
  private Node             eventsNode ;
  
  public TXEventManager(Registry registry, String eventPath) throws RegistryException {
    this.registry = registry;
    this.eventPath = eventPath;
    eventsNode = registry.createIfNotExist(eventPath + "/events") ;
  }
  
  public String getEventPath() { return this.eventPath ; }
  
  public Registry getRegistry() { return this.registry ; }
  
  public void broadcast(TXEvent event) throws RegistryException {
    eventsNode.createChild(event.getName(), event, NodeCreateMode.PERSISTENT);
  }
  
  public void broadcast(TXEvent event, TXEventNotificationListener listener) throws RegistryException {
    eventsNode.createChild(event.getName(), event, NodeCreateMode.PERSISTENT);
    
  }

  public void notify(TXEvent event, TXEventNotification notification) throws RegistryException {
    Node eventNode = eventsNode.getChild(event.getName());
    eventNode.createChild("notitification-", notification, NodeCreateMode.PERSISTENT_SEQUENTIAL);
  }
}
