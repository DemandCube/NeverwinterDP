package com.neverwinterdp.nstorage.test;

import java.util.List;

import com.neverwinterdp.util.text.TabularFormater;

public class TrackingRecordReport {
  private String writerId = "unknown";
  private String chunkId;
  private int    numOfMessage;
  private int    progress;
  private int    noLostTo;
  private int    lostCount;
  private int    duplicatedCount;
  
  public TrackingRecordReport() {} 
  
  public String getwriterId() { return writerId; }
  public void setWriterId(String writerId) { this.writerId = writerId; }

  public String getChunkId() { return chunkId; }
  public void   setChunkId(String chunkId) { this.chunkId = chunkId; }
  
  public int getNumOfMessage() { return numOfMessage; }
  public void setNumOfRecord(int numOfMessage) { this.numOfMessage = numOfMessage; }

  public int getProgress() { return progress; }
  public void setProgress(int progress) { this.progress = progress; }

  public int getNoLostTo() { return noLostTo;}
  public void setNoLostTo(int noLostTo) { this.noLostTo = noLostTo;}

  public int getLostCount() { return lostCount; }
  public void setLostCount(int lostCount) { this.lostCount = lostCount; }

  public int getDuplicatedCount() { return duplicatedCount; }
  public void setDuplicatedCount(int duplicatedCount) { this.duplicatedCount = duplicatedCount; }
  
  public String reportName() { return writerId + "." + chunkId; }
  
  static public String getFormattedReport(String title, List<TrackingRecordReport> reports) {
    String[] header = {
      "Chunk Id", "Writer Id", "Num Of Message", "Progress", "No Lost To", "Lost", "Duplicated"
    };
    TabularFormater formater = new TabularFormater(header);
    formater.setTitle(title);
    for(TrackingRecordReport report : reports) {
      formater.addRow(
        report.getChunkId(), report.getwriterId(), report.getNumOfMessage(), report.getProgress(), 
        report.getNoLostTo(), report.getLostCount(), report.getDuplicatedCount()
      );
    }
    return formater.getFormattedText() ;
  }
}
