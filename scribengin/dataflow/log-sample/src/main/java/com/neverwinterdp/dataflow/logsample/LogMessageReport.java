package com.neverwinterdp.dataflow.logsample;

import java.util.List;

import com.neverwinterdp.util.text.TabularFormater;

public class LogMessageReport {
  private String groupId ;
  private int    numOfMessage ;
  private int    lostCount ;
  private int    duplicatedCount ;
  
  public LogMessageReport() {} 
  
  public LogMessageReport(String groupId, int numOfMessage, int lostCount, int duplicatedCount) {
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
  
  static public String getFormattedReport(String title, List<LogMessageReport> reports) {
    TabularFormater formater = new TabularFormater("Group Id", "Num Of Message", "Lost", "Duplicated");
    formater.setTitle(title);
    for(LogMessageReport report : reports) {
      formater.addRow(report.getGroupId(), report.getNumOfMessage(), report.getLostCount(), report.getDuplicatedCount());
    }
    return formater.getFormattedText() ;
  }
}
