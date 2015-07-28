package com.neverwinterdp.registry.txevent;

public interface TXEventNotificationListener {
  
  public void onNotification(TXEvent event, TXEventNotification notification) throws Exception ;
  
}
