package com.neverwinterdp.registry.txevent;

public class TXEventNotificationCompleteListener implements TXEventNotificationListener {
  @Override
  public void onNotification(TXEvent event, TXEventNotification notification) throws Exception {
    if(notification.getStatus() !=  TXEventNotification.Status.Complete) {
      throw new Exception("Expect complete status for " + notification.getClientId() + ", but " + notification.getStatus());
    }
  }
}
