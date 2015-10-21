package com.neverwinterdp.scribengin.dataflow.tool.tracking;

import java.util.List;

import com.neverwinterdp.tool.message.BitSetMessageTracker;
import com.neverwinterdp.util.text.TabularFormater;

public class TrackingMessageReport {
  private String vmId = "unknown";
  private String chunkId;
  private int    numOfMessage;
  private int    progress;
  private int    noLostTo;
  private int    lostCount;
  private int    duplicatedCount;

  public TrackingMessageReport() {} 
  
  public TrackingMessageReport(String vmId, String chunkId, int numOfMessage) {
    this.vmId = vmId;
    this.chunkId = chunkId ;
    this.numOfMessage = numOfMessage;
  }
  
  public TrackingMessageReport(String chunkId, int numOfMessage, int progress, int noLostTo, int lostCount, int duplicatedCount) {
    this.chunkId = chunkId ;
    this.numOfMessage = numOfMessage;
    this.progress = progress ;
    this.noLostTo = noLostTo;
    this.lostCount = lostCount;
    this.duplicatedCount = duplicatedCount;
  }
  
  public TrackingMessageReport(String chunkId, BitSetMessageTracker.BitSetPartitionMessageTracker tracker) {
    this.chunkId = chunkId ;
    BitSetMessageTracker.BitSetPartitionMessageReport report = tracker.getReport();
    this.numOfMessage = report.getNumOfBits();
    this.progress     = report.getTrackProgress() + 1;
    this.noLostTo     = report.getNoLostTo();
    this.lostCount = report.getLostCount();
    this.duplicatedCount = report.getDuplicatedCount();
  }
  
  public String getVmId() { return vmId; }
  public void setVmId(String vmId) { this.vmId = vmId; }

  public String getChunkId() { return chunkId; }
  public void   setChunkId(String chunkId) { this.chunkId = chunkId; }
  
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
  
  public String reportName() { return vmId + "." + chunkId; }
  
  static public String getFormattedReport(String title, List<TrackingMessageReport> reports) {
    
    TabularFormater formater = new TabularFormater("Chunk Id", "Num Of Message", "Progress", "No Lost To", "Lost", "Duplicated");
    formater.setTitle(title);
    for(TrackingMessageReport report : reports) {
      formater.addRow(
        report.getChunkId(), report.getNumOfMessage(), report.getProgress(), 
        report.getNoLostTo(), report.getLostCount(), report.getDuplicatedCount()
      );
    }
    return formater.getFormattedText() ;
  }
}
