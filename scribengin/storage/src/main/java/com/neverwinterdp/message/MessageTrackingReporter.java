package com.neverwinterdp.message;

import java.util.ArrayList;
import java.util.List;

import com.neverwinterdp.util.text.TabularFormater;

public class MessageTrackingReporter {
  private String                                  name;
  private List<AggregateMessageTrackingChunkStat> aggregateChunkReports;
  
  public MessageTrackingReporter() { }
  
  public MessageTrackingReporter(String name) {
    this.name                  = name ;
    this.aggregateChunkReports = new ArrayList<>();
  }
  
  public String getName() { return name; }
  public void setName(String name) { this.name = name; }

  public List<AggregateMessageTrackingChunkStat> getAggregateChunkReports() { return aggregateChunkReports; }
  public void setAggregateChunkReports(List<AggregateMessageTrackingChunkStat> aggregateChunkReports) {
    this.aggregateChunkReports = aggregateChunkReports;
  }

  public void merge(MessageTrackingChunkStat chunk) {
    for(int i = 0; i < aggregateChunkReports.size(); i++) {
      AggregateMessageTrackingChunkStat sel = aggregateChunkReports.get(i) ;
      if(chunk.getChunkId() < sel.getFromChunkId()) {
        AggregateMessageTrackingChunkStat newChunkReport = new AggregateMessageTrackingChunkStat();
        newChunkReport.merge(chunk);
        aggregateChunkReports.add(i, newChunkReport);
        return;
      } else if(chunk.getChunkId() == sel.getToChunkId() + 1) {
        sel.merge(chunk);
        return;
      }
    }
    AggregateMessageTrackingChunkStat newChunkReport = new AggregateMessageTrackingChunkStat();
    newChunkReport.merge(chunk);
    aggregateChunkReports.add(newChunkReport);
  }
  
  public void optimize() {
    List<AggregateMessageTrackingChunkStat> holder = new ArrayList<>();
    AggregateMessageTrackingChunkStat previous = null;
    for(int i = 0; i < aggregateChunkReports.size(); i++) {
      AggregateMessageTrackingChunkStat current = aggregateChunkReports.get(i) ;
      if(previous == null) {
        previous = current;
        holder.add(current);
      } else if(previous.getToChunkId() + 1 == current.getFromChunkId()) {
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
      AggregateMessageTrackingChunkStat sel = aggregateChunkReports.get(i) ;
      ft.addRow(sel.getFromChunkId(), sel.getToChunkId(), sel.getTrackingLostCount(), sel.getTrackingDuplicatedCount());
    }
    return ft.getFormattedText();
  }
}
