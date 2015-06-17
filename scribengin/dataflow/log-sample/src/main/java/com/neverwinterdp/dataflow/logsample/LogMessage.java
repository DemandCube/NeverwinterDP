package com.neverwinterdp.dataflow.logsample;

public class LogMessage {
  private String groupId ;
  private int    sequenceId ;
  private byte[] message ;
  
  public LogMessage() {}
  
  public LogMessage(String groupId, int sequenceId, int messageSize) {
    this.groupId       = groupId;
    this.sequenceId =  sequenceId;
    this.message    = new byte[messageSize];
  }

  public String getGroupId() { return groupId; }
  public void setGroupId(String groupId) { this.groupId = groupId; }
  
  public int getSequenceId() { return sequenceId; }
  public void setSequenceId(int sequenceId) { this.sequenceId = sequenceId; }
  
  public byte[] getMessage() { return message; }
  public void setMessage(byte[] message) { this.message = message; }
}
