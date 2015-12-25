package com.neverwinterdp.message;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.neverwinterdp.util.text.TabularFormater;

public class MessageTrackingReporter {
  private String                                  name;
  private List<AggregateMessageTrackingChunkStat> finishedAggregateChunkReports;
  private List<AggregateMessageTrackingChunkStat> progressAggregateChunkReports;
  
  public MessageTrackingReporter() { }
  
  public MessageTrackingReporter(String name) {
    this.name                  = name ;
    finishedAggregateChunkReports = new ArrayList<>();
    progressAggregateChunkReports = new ArrayList<>();
  }
  
  public String getName() { return name; }
  public void setName(String name) { this.name = name; }

  public List<AggregateMessageTrackingChunkStat> getFinishedAggregateChunkReports() { 
    return finishedAggregateChunkReports; 
  }
  
  public void setFinishedAggregateChunkReports(List<AggregateMessageTrackingChunkStat> aggregateChunkReports) {
    finishedAggregateChunkReports = aggregateChunkReports;
  }

  public List<AggregateMessageTrackingChunkStat> getProgressAggregateChunkReports() { 
    return progressAggregateChunkReports; 
  }
  
  public void setProgressAggregateChunkReports(List<AggregateMessageTrackingChunkStat> aggregateChunkReports) {
    progressAggregateChunkReports = aggregateChunkReports;
  }
  
  public long getLogNameCount(String logName) {
    long count = getLogNameCount(finishedAggregateChunkReports, logName);
    count += getLogNameCount(progressAggregateChunkReports, logName);
    return count;
  }
  
  long getLogNameCount(List<AggregateMessageTrackingChunkStat> chunkReportHolder, String logName) {
    long count = 0;
    for(int i = 0; i < chunkReportHolder.size(); i++) {
      AggregateMessageTrackingChunkStat stat = chunkReportHolder.get(i);
      count += stat.getLogNameCount(logName);
    }
    return count;
  }
  
  public void mergeFinished(MessageTrackingChunkStat chunk) {
    merge(finishedAggregateChunkReports, chunk);
  }
  
  public void mergeProgress(MessageTrackingChunkStat chunk) {
    merge(progressAggregateChunkReports, chunk);
  }
  
  void merge(List<AggregateMessageTrackingChunkStat> reportChunkHolder, MessageTrackingChunkStat chunk) {
    for(int i = 0; i < reportChunkHolder.size(); i++) {
      AggregateMessageTrackingChunkStat sel = reportChunkHolder.get(i) ;
      if(chunk.getChunkId() < sel.getFromChunkId()) {
        AggregateMessageTrackingChunkStat newChunkReport = new AggregateMessageTrackingChunkStat();
        newChunkReport.merge(chunk);
        reportChunkHolder.add(i, newChunkReport);
        return;
      } else if(chunk.getChunkId() == sel.getToChunkId() + 1) {
        sel.merge(chunk);
        return;
      }
    }
    AggregateMessageTrackingChunkStat newChunkReport = new AggregateMessageTrackingChunkStat();
    newChunkReport.merge(chunk);
    reportChunkHolder.add(newChunkReport);
  }
  
  public void optimize() {
    finishedAggregateChunkReports = optimize(finishedAggregateChunkReports);
  }
  
  List<AggregateMessageTrackingChunkStat> optimize(List<AggregateMessageTrackingChunkStat> reportChunkHolder) {
    List<AggregateMessageTrackingChunkStat> newHolder = new ArrayList<>();
    AggregateMessageTrackingChunkStat previous = null;
    for(int i = 0; i < reportChunkHolder.size(); i++) {
      AggregateMessageTrackingChunkStat current = reportChunkHolder.get(i) ;
      if(previous == null) {
        previous = current;
        newHolder.add(current);
      } else if(previous.getToChunkId() + 1 == current.getFromChunkId()) {
        previous.merge(current);
      } else {
        previous = current;
        newHolder.add(current);
      }
    }
    return newHolder;
  }
  
  public String toFormattedText() {
    StringBuilder b = new StringBuilder();
    b.append(toFormattedText("Finishied " + name + " report", finishedAggregateChunkReports));
    b.append(toFormattedText("Progress " + name + " report", progressAggregateChunkReports));
    return b.toString();
  }
  
  String toFormattedText(String title, List<AggregateMessageTrackingChunkStat> reportChunkHolder) {
    TabularFormater ft = new TabularFormater("From - To", "Stat", "Lost", "Duplicated", "Count", "Avg Delivery");
    ft.setTitle(title);
    for(int i = 0; i < reportChunkHolder.size(); i++) {
      AggregateMessageTrackingChunkStat sel = reportChunkHolder.get(i) ;
      ft.addRow(sel.getFromChunkId() + " - " + sel.getToChunkId(), "", "", "", "", "");
      ft.addRow("", "Tracking", sel.getTrackingLostCount(), sel.getTrackingDuplicatedCount(), sel.getTrackingCount(), "");
      Map<String, MessageTrackingLogChunkStat> logStats = sel.getLogStats();
      List<String> logStatKeys = new ArrayList<>(logStats.keySet());
      Collections.sort(logStatKeys);
      for(String logName : logStatKeys) {
        MessageTrackingLogChunkStat selLogChunkStat = logStats.get(logName);
        ft.addRow("", logName, "", "", selLogChunkStat.getCount(), selLogChunkStat.getAvgDeliveryTime());
      }
    }
    return ft.getFormattedText();
  }
}