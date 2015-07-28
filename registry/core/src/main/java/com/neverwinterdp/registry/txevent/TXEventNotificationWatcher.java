package com.neverwinterdp.registry.txevent;

public class TXEventNotificationWatcher {
  private TXEvent event; 
  private TXEventNotificationListener listener ;
  
  public TXEventNotificationWatcher(TXEventManager manager, TXEvent event, TXEventNotificationListener listener) {
    this.event = event;
    this.listener = listener ;
  }
  
  
}
