package com.neverwinterdp.scribengin.dataflow.tool.tracking;

import java.util.List;

import com.neverwinterdp.tool.message.BitSetMessageTracker;
import com.neverwinterdp.util.text.TabularFormater;

public class TrackingReport {
  private String groupId ;
  private int    numOfMessage ;
  private int    progress ;
  private int    noLostTo;
  private int    lostCount ;
  private int    duplicatedCount ;
  
  public TrackingReport() {} 
  
  public TrackingReport(String groupId, int numOfMessage, int progress, int noLostTo, int lostCount, int duplicatedCount) {
    this.groupId = groupId ;
    this.numOfMessage = numOfMessage;
    this.progress = progress ;
    this.noLostTo = noLostTo;
    this.lostCount = lostCount;
    this.duplicatedCount = duplicatedCount;
  }
  
  public TrackingReport(String groupId, BitSetMessageTracker.BitSetPartitionMessageTracker tracker) {
    this.groupId = groupId ;
    BitSetMessageTracker.BitSetPartitionMessageReport report = tracker.getReport();
    this.numOfMessage = report.getNumOfBits();
    this.progress     = report.getTrackProgress() + 1;
    this.noLostTo     = report.getNoLostTo();
    this.lostCount = report.getLostCount();
    this.duplicatedCount = report.getDuplicatedCount();
  }
  
  public String getGroupId() { return groupId; }
  public void   setGroupId(String groupId) { this.groupId = groupId; }
  
  public int getNumOfMessage() { return numOfMessage; }
  public void setNumOfMessage(int numOfMessage) { this.numOfMessage = numOfMessage; }

  public int getProgress() { return progress; }
  public void setProgress(int progress) { this.progress = progress; }

  public int getNoLostTo() { return noLostTo;}
  public void setNoLostTo(int noLostTo) { this.noLostTo = noLostTo;}

  public int getLostCount() { return lostCount; }
  public void setLostCount(int lostCount) { this.lostCount = lostCount; }

  public int getDuplicatedCount() { return duplicatedCount; }
  public void setDuplicatedCount(int duplicatedCount) { this.duplicatedCount = duplicatedCount; }
  
  static public String getFormattedReport(String title, List<TrackingReport> reports) {
    TabularFormater formater = new TabularFormater("Group Id", "Num Of Message", "Progress", "No Lost To", "Lost", "Duplicated");
    formater.setTitle(title);
    for(TrackingReport report : reports) {
      formater.addRow(
        report.getGroupId(), report.getNumOfMessage(), report.getProgress(), 
        report.getLostCount(), report.getLostCount(), report.getDuplicatedCount()
      );
    }
    return formater.getFormattedText() ;
  }
}
