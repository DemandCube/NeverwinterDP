package com.neverwinterdp.registry;

public enum ErrorCode {
  Connection(1, "Cannot connect to the registry server due to the error such the unavailable server, timeout.."),
  Timeout(2, "Cannot wait for an event, or wait too long"),
  ConnectionLoss(3, "Loose the connection due to timeout or broken network"),
  
  NodeCreation(10, "Cannot create node due to certain error such, no parent node..."),
  NodeAccess(11, "Cannot access the node due to certain error such permission"),
  NoNode(12, "No such node exists"),
  NodeExists(13, "A node is already existed"),
  
  Closed(20, "The Registry object has been closed and no longer usable"),
  
  Unknown(1000,  "Unclassified error");
  
  private int code;
  private String description ;

  ErrorCode(int code, String description) {
      this.code = code;
      this.description = description ;
  }
  
  public int getCode() { return this.code ; }
  
  public boolean isConnectionProblem() { return code < 10; }
  
  public boolean isOperationProblem() { return code >= 10 && code < 20; }
  
  public String getDescription() { return this.description ; }
}
