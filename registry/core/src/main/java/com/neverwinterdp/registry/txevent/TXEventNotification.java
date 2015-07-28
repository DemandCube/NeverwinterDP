package com.neverwinterdp.registry.txevent;

public class TXEventNotification {
  static public enum Status { Received, Complete, Abort }

  private String       clientId ;
  private Status       status;
  
  public String getClientId() { return clientId; }
  public void setClientId(String clientId) { this.clientId = clientId; }
  
  public Status getStatus() { return status; }
  public void setStatus(Status status) { this.status = status; }
}
