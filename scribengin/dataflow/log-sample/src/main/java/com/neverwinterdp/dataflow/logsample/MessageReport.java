package com.neverwinterdp.dataflow.logsample;

import java.util.List;

import com.neverwinterdp.util.text.TabularFormater;

public class MessageReport {
  private String groupId ;
  private int    numOfMessage ;
  private int    lostCount ;
  private int    duplicatedCount ;
  
  public MessageReport() {} 
  
  public MessageReport(String groupId, int numOfMessage, int lostCount, int duplicatedCount) {
    this.groupId = groupId ;
    this.numOfMessage = numOfMessage;
    this.lostCount = lostCount;
    this.duplicatedCount = duplicatedCount;
  }
  
  public String getGroupId() { return groupId; }
  public void   setGroupId(String groupId) { this.groupId = groupId; }
  
  public int getNumOfMessage() { return numOfMessage; }
  public void setNumOfMessage(int numOfMessage) { this.numOfMessage = numOfMessage; }

  public int getLostCount() { return lostCount; }
  public void setLostCount(int lostCount) { this.lostCount = lostCount; }

  public int getDuplicatedCount() { return duplicatedCount; }
  public void setDuplicatedCount(int duplicatedCount) { this.duplicatedCount = duplicatedCount; }
  
  static public String getFormattedReport(String title, List<MessageReport> reports) {
    TabularFormater formater = new TabularFormater("Group Id", "Num Of Message", "Lost", "Duplicated");
    formater.setTitle(title);
    for(MessageReport report : reports) {
      formater.addRow(report.getGroupId(), report.getNumOfMessage(), report.getLostCount(), report.getDuplicatedCount());
    }
    return formater.getFormattedText() ;
  }
}
