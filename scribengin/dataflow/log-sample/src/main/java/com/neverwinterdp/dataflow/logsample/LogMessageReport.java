package com.neverwinterdp.dataflow.logsample;

public class LogMessageReport {
  private String groupId ;
  private int    numOfMessage ;
  
  public LogMessageReport() {} 
  
  public LogMessageReport(String groupId, int numOfMessage) {
    this.groupId = groupId ;
    this.numOfMessage = numOfMessage;
  }
  
  public String getGroupId() { return groupId; }
  public void   setGrouId(String groupId) { this.groupId = groupId; }
  
  public int getNumOfMessage() { return numOfMessage; }
  public void setNumOfMessage(int numOfMessage) { this.numOfMessage = numOfMessage; }
}
