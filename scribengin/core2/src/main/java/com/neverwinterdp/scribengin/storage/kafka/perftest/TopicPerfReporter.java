package com.neverwinterdp.scribengin.storage.kafka.perftest;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.neverwinterdp.os.OSManagement;
import com.neverwinterdp.util.text.TabularFormater;

public class TopicPerfReporter {
  private long                         startTime    = System.currentTimeMillis();
  private OSManagement                 osManagement = new OSManagement();
  private Map<String, TopicPerfReport> reports      = new HashMap<>();

  public String getFormattedText() {
    StringBuilder fText = new StringBuilder();
    long elapsedTime = System.currentTimeMillis() - startTime;
    long elapsedTimeInSec = TimeUnit.MILLISECONDS.toSeconds(elapsedTime);
    TabularFormater formater = new TabularFormater("Topic", "Write", "Read", "Elapsed Time(sec)");
    formater.setTitle("Topics Report");
    synchronized(reports) {
      for(TopicPerfReport sel : reports.values()) {
        formater.addRow(sel.topic, sel.write.get(), sel.read.get(), elapsedTimeInSec);
      }
    }
    fText.append(formater.getFormattedText()).append("\n\n");
    fText.append(osManagement.getOSInfoFormattedText()).append("\n\n");
    fText.append(osManagement.getMemoryInfoFormattedText()).append("\n\n");
    fText.append(osManagement.getGCInfoFormattedText()).append("\n\n");
    return fText.toString();
  }
  
  public void incrRead(String topic, int incr) {
    getTopicPerfReport(topic).incrRead(incr);
  }
  
  public void incrWrite(String topic, int incr) {
    getTopicPerfReport(topic).incrWrite(incr);
  }
  
  TopicPerfReport getTopicPerfReport(String topic) {
    TopicPerfReport report = reports.get(topic);
    if(report == null) {
      synchronized(reports) {
        report = reports.get(topic);
        if(report == null) {
          report = new TopicPerfReport(topic);
          reports.put(topic, report);
        }
      }
    }
    return report;
  }
  
  public class TopicPerfReport {
    private String        topic;
    private AtomicInteger read = new AtomicInteger() ;
    private AtomicInteger write = new AtomicInteger() ; 
    
    public TopicPerfReport(String topic) {
      this.topic = topic;
    }
    
    public void incrRead(int incr) {
      read.addAndGet(incr);
    }
    
    public void incrWrite(int incr) {
      write.addAndGet(incr);
    }
    
  }
  
}
