package com.neverwinterdp.message;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.neverwinterdp.util.text.TabularFormater;

public class TrackingWindowReport {
  private String            name;
  private List<TrackingWindowStats> finishedWindowStats;
  private List<TrackingWindowStats> progressWindowStats;
  
  public TrackingWindowReport() { }
  
  public TrackingWindowReport(String name) {
    this.name                  = name ;
    finishedWindowStats = new ArrayList<>();
    progressWindowStats = new ArrayList<>();
  }
  
  public String getName() { return name; }
  public void setName(String name) { this.name = name; }

  public List<TrackingWindowStats> getFinishedWindowStats() {  return finishedWindowStats;  }
  public void setFinishedWindowStats(List<TrackingWindowStats> stats) { finishedWindowStats = stats; }

  public List<TrackingWindowStats> getProgressWindowStats() { return progressWindowStats;  }
  public void setProgressWindowStats(List<TrackingWindowStats> stats) { progressWindowStats = stats; }
  
  @JsonIgnore
  public int getInSequenceCompleteWindowCount() {
    if(finishedWindowStats.size() == 0) return 0;
    return finishedWindowStats.get(0).getToWindowId();
  }
  
  @JsonIgnore
  public int getCompleteWindowCount() {
    int count = 0 ;
    for(int i = 0; i < finishedWindowStats.size(); i++) {
      TrackingWindowStats stats = finishedWindowStats.get(i);
      count += (stats.getToWindowId() - stats.getFromWindowId()) + 1;
    }
    return count;
  }
  
  @JsonIgnore
  public long getTrackingCount() {
    long count = getTrackingCount(finishedWindowStats);
    count += getTrackingCount(progressWindowStats);
    return count;
  }

  long getTrackingCount(List<TrackingWindowStats> chunkReportHolder) {
    long count = 0;
    for(int i = 0; i < chunkReportHolder.size(); i++) {
      TrackingWindowStats stat = chunkReportHolder.get(i);
      count += stat.getTrackingCount();
    }
    return count;
  }
  
  public long getLogNameCount(String logName) {
    long count = getLogNameCount(finishedWindowStats, logName);
    count += getLogNameCount(progressWindowStats, logName);
    return count;
  }
  
  long getLogNameCount(List<TrackingWindowStats> chunkReportHolder, String logName) {
    long count = 0;
    for(int i = 0; i < chunkReportHolder.size(); i++) {
      TrackingWindowStats stat = chunkReportHolder.get(i);
      count += stat.getLogNameCount(logName);
    }
    return count;
  }
  
  public void mergeFinished(TrackingWindowStat stat) {
    merge(finishedWindowStats, stat);
  }
  
  public void mergeProgress(TrackingWindowStat stat) {
    merge(progressWindowStats, stat);
  }
  
  void merge(List<TrackingWindowStats> reportChunkHolder, TrackingWindowStat chunk) {
    for(int i = 0; i < reportChunkHolder.size(); i++) {
      TrackingWindowStats sel = reportChunkHolder.get(i) ;
      if(chunk.getWindowId() < sel.getFromWindowId()) {
        TrackingWindowStats newChunkReport = new TrackingWindowStats();
        newChunkReport.merge(chunk);
        reportChunkHolder.add(i, newChunkReport);
        return;
      } else if(chunk.getWindowId() == sel.getToWindowId() + 1) {
        sel.merge(chunk);
        return;
      }
    }
    TrackingWindowStats newChunkReport = new TrackingWindowStats();
    newChunkReport.merge(chunk);
    reportChunkHolder.add(newChunkReport);
  }
  
  public void optimize() {
    finishedWindowStats = optimize(finishedWindowStats);
  }
  
  List<TrackingWindowStats> optimize(List<TrackingWindowStats> reportChunkHolder) {
    List<TrackingWindowStats> newHolder = new ArrayList<>();
    TrackingWindowStats previous = null;
    for(int i = 0; i < reportChunkHolder.size(); i++) {
      TrackingWindowStats current = reportChunkHolder.get(i) ;
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
    b.append(toFormattedText("Finished " + name + " report", finishedWindowStats, false));
    b.append(toFormattedText("Progress " + name + " report", progressWindowStats, false));
    return b.toString();
  }
  
  public String toDetailFormattedText() {
    StringBuilder b = new StringBuilder();
    b.append(toFormattedText("Finished " + name + " report", finishedWindowStats, true));
    b.append(toFormattedText("Progress " + name + " report", progressWindowStats, true));
    return b.toString();
  }
  
  String toFormattedText(String title, List<TrackingWindowStats> windowStats, boolean detail) {
    TabularFormater ft = new TabularFormater("From - To", "Stat", "Lost", "Duplicated", "Count", "Avg Delivery");
    ft.setTitle(title);
    for(int i = 0; i < windowStats.size(); i++) {
      TrackingWindowStats sel = windowStats.get(i) ;
      ft.addRow(sel.getFromWindowId() + " - " + sel.getToWindowId(), "", "", "", "", "");
      ft.addRow("", "Tracking", sel.getTrackingLostCount(), sel.getTrackingDuplicatedCount(), sel.getTrackingCount(), "");
      if(detail) {
        Map<String, TrackingWindowLogStat> logStats = sel.getLogStats();
        List<String> logStatKeys = new ArrayList<>(logStats.keySet());
        Collections.sort(logStatKeys);
        for(String logName : logStatKeys) {
          TrackingWindowLogStat selLogChunkStat = logStats.get(logName);
          ft.addRow("", logName, "", "", selLogChunkStat.getCount(), selLogChunkStat.getAvgDeliveryTime());
        }
      }
    }
    return ft.getFormattedText();
  }
}