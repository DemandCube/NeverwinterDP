package com.neverwinterdp.message;

import java.util.ArrayList;
import java.util.List;

import com.neverwinterdp.util.text.TabularFormater;

public class MessageTrackingLogReporter {
  private String name ;
  private List<AggregateChunkReport> aggregateChunkReports;
  
  
  public MessageTrackingLogReporter() { }
  
  public MessageTrackingLogReporter(String name) {
    this.name                  = name ;
    this.aggregateChunkReports = new ArrayList<>();
  }
  
  public String getName() { return name; }
  public void setName(String name) { this.name = name; }

  public List<AggregateChunkReport> getAggregateChunkReports() { return aggregateChunkReports; }
  public void setAggregateChunkReports(List<AggregateChunkReport> aggregateChunkReports) {
    this.aggregateChunkReports = aggregateChunkReports;
  }

  public void merge(MessageTrackingLogChunk chunk) {
    for(int i = 0; i < aggregateChunkReports.size(); i++) {
      AggregateChunkReport sel = aggregateChunkReports.get(i) ;
      if(sel.getFromChunkId() > chunk.getChunkId()) {
        AggregateChunkReport newChunkReport = new AggregateChunkReport();
        newChunkReport.merge(chunk);
        aggregateChunkReports.add(i, newChunkReport);
        return;
      } else if(sel.getToChunkId() == chunk.getChunkId()) {
        sel.merge(chunk);
        return;
      }
    }
    AggregateChunkReport newChunkReport = new AggregateChunkReport();
    newChunkReport.merge(chunk);
    aggregateChunkReports.add(newChunkReport);
  }
  
  public void optimize() {
    List<AggregateChunkReport> holder = new ArrayList<>();
    AggregateChunkReport previous = null;
    for(int i = 0; i < aggregateChunkReports.size(); i++) {
      AggregateChunkReport current = aggregateChunkReports.get(i) ;
      if(previous == null) {
        previous = current;
        holder.add(current);
      } else if(previous.getToChunkId() == current.getFromChunkId()) {
        previous.merge(current);
      } else {
        previous = current;
        holder.add(current);
      }
    }
    aggregateChunkReports = holder;
  }
  
  public String toFormattedText() {
    TabularFormater ft = new TabularFormater("From", "To", "Lost", "Duplicated");
    ft.setTitle(name + " report");
    for(int i = 0; i < aggregateChunkReports.size(); i++) {
      AggregateChunkReport sel = aggregateChunkReports.get(i) ;
      ft.addRow(sel.getFromChunkId(), sel.getToChunkId(), sel.getTrackingLostCount(), sel.getTrackingDuplicatedCount());
    }
    return ft.getFormattedText();
  }
  
  static public class AggregateChunkReport {
    private int fromChunkId = -1;
    private int toChunkId   = -1;
    private long trackingLostCount;
    private long trackingDuplicatedCount;
    
    public int getFromChunkId() { return fromChunkId;}
    public void setFromChunkId(int fromChunkId) {
      this.fromChunkId = fromChunkId;
    }

    public int getToChunkId() { return toChunkId; }
    public void setToChunkId(int toChunkId) {
      this.toChunkId = toChunkId;
    }

    public long getTrackingLostCount() { return trackingLostCount; }
    public void setTrackingLostCount(long trackingLostCount) {
      this.trackingLostCount = trackingLostCount;
    }

    public long getTrackingDuplicatedCount() { return trackingDuplicatedCount; }
    public void setTrackingDuplicatedCount(long trackingDuplicatedCount) {
      this.trackingDuplicatedCount = trackingDuplicatedCount;
    }

    public void merge(MessageTrackingLogChunk chunk) {
      if(fromChunkId < 0) fromChunkId = chunk.getChunkId();
      toChunkId = chunk.getChunkId() + 1;
      trackingLostCount += chunk.getTrackingLostCount();
      trackingDuplicatedCount += chunk.getTrackingDuplicatedCount();
    }
    
    public void merge(AggregateChunkReport other) {
      toChunkId = other.getToChunkId();
      trackingLostCount += other.getTrackingLostCount();
      trackingDuplicatedCount += other.getTrackingDuplicatedCount();
    }
  }
}
