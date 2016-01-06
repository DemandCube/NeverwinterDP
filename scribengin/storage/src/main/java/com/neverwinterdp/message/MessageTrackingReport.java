package com.neverwinterdp.message;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.neverwinterdp.util.text.TabularFormater;

public class MessageTrackingReport {
  private String                           name;
  private List<WindowMessageTrackingStats> finishedWindowStats;
  private List<WindowMessageTrackingStats> progressWindowStats;
  
  public MessageTrackingReport() { }
  
  public MessageTrackingReport(String name) {
    this.name                  = name ;
    finishedWindowStats = new ArrayList<>();
    progressWindowStats = new ArrayList<>();
  }
  
  public String getName() { return name; }
  public void setName(String name) { this.name = name; }

  public List<WindowMessageTrackingStats> getFinishedWindowStats() { 
    return finishedWindowStats; 
  }
  
  public void setFinishedWindowStats(List<WindowMessageTrackingStats> stats) {
    finishedWindowStats = stats;
  }

  public List<WindowMessageTrackingStats> getProgressWindowStats() { 
    return progressWindowStats; 
  }
  
  public void setProgressWindowStats(List<WindowMessageTrackingStats> stats) {
    progressWindowStats = stats;
  }
  
  @JsonIgnore
  public int getInSequenceCompleteWindow() {
    if(finishedWindowStats.size() == 0) return 0;
    return finishedWindowStats.get(0).getToWindowId();
  }
  
  @JsonIgnore
  public long getTrackingCount() {
    long count = getTrackingCount(finishedWindowStats);
    count += getTrackingCount(progressWindowStats);
    return count;
  }

  long getTrackingCount(List<WindowMessageTrackingStats> chunkReportHolder) {
    long count = 0;
    for(int i = 0; i < chunkReportHolder.size(); i++) {
      WindowMessageTrackingStats stat = chunkReportHolder.get(i);
      count += stat.getTrackingCount();
    }
    return count;
  }
  
  public long getLogNameCount(String logName) {
    long count = getLogNameCount(finishedWindowStats, logName);
    count += getLogNameCount(progressWindowStats, logName);
    return count;
  }
  
  long getLogNameCount(List<WindowMessageTrackingStats> chunkReportHolder, String logName) {
    long count = 0;
    for(int i = 0; i < chunkReportHolder.size(); i++) {
      WindowMessageTrackingStats stat = chunkReportHolder.get(i);
      count += stat.getLogNameCount(logName);
    }
    return count;
  }
  
  public void mergeFinished(WindowMessageTrackingStat stat) {
    merge(finishedWindowStats, stat);
  }
  
  public void mergeProgress(WindowMessageTrackingStat stat) {
    merge(progressWindowStats, stat);
  }
  
  void merge(List<WindowMessageTrackingStats> reportChunkHolder, WindowMessageTrackingStat chunk) {
    for(int i = 0; i < reportChunkHolder.size(); i++) {
      WindowMessageTrackingStats sel = reportChunkHolder.get(i) ;
      if(chunk.getWindowId() < sel.getFromWindowId()) {
        WindowMessageTrackingStats newChunkReport = new WindowMessageTrackingStats();
        newChunkReport.merge(chunk);
        reportChunkHolder.add(i, newChunkReport);
        return;
      } else if(chunk.getWindowId() == sel.getToWindowId() + 1) {
        sel.merge(chunk);
        return;
      }
    }
    WindowMessageTrackingStats newChunkReport = new WindowMessageTrackingStats();
    newChunkReport.merge(chunk);
    reportChunkHolder.add(newChunkReport);
  }
  
  public void optimize() {
    finishedWindowStats = optimize(finishedWindowStats);
  }
  
  List<WindowMessageTrackingStats> optimize(List<WindowMessageTrackingStats> reportChunkHolder) {
    List<WindowMessageTrackingStats> newHolder = new ArrayList<>();
    WindowMessageTrackingStats previous = null;
    for(int i = 0; i < reportChunkHolder.size(); i++) {
      WindowMessageTrackingStats current = reportChunkHolder.get(i) ;
      if(previous == null) {
        previous = current;
        newHolder.add(current);
      } else if(previous.getToWindowId() + 1 == current.getFromWindowId()) {
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
    b.append(toFormattedText("Finished " + name + " report", finishedWindowStats));
    b.append(toFormattedText("Progress " + name + " report", progressWindowStats));
    return b.toString();
  }
  
  String toFormattedText(String title, List<WindowMessageTrackingStats> reportChunkHolder) {
    TabularFormater ft = new TabularFormater("From - To", "Stat", "Lost", "Duplicated", "Count", "Avg Delivery");
    ft.setTitle(title);
    for(int i = 0; i < reportChunkHolder.size(); i++) {
      WindowMessageTrackingStats sel = reportChunkHolder.get(i) ;
      ft.addRow(sel.getFromWindowId() + " - " + sel.getToWindowId(), "", "", "", "", "");
      ft.addRow("", "Tracking", sel.getTrackingLostCount(), sel.getTrackingDuplicatedCount(), sel.getTrackingCount(), "");
      Map<String, WindowMessageTrackingLogStat> logStats = sel.getLogStats();
      List<String> logStatKeys = new ArrayList<>(logStats.keySet());
      Collections.sort(logStatKeys);
      for(String logName : logStatKeys) {
        WindowMessageTrackingLogStat selLogChunkStat = logStats.get(logName);
        ft.addRow("", logName, "", "", selLogChunkStat.getCount(), selLogChunkStat.getAvgDeliveryTime());
      }
    }
    return ft.getFormattedText();
  }
}