package com.neverwinterdp.message;

import java.util.HashMap;
import java.util.Map;

public class AllMessageTrackingReporter {
  private Map<String, MessageTrackingReporter> messageTrackingReporters ;
  
  public AllMessageTrackingReporter() {
  }
  
  public AllMessageTrackingReporter(boolean init) {
    messageTrackingReporters = new HashMap<>();
  }

  public Map<String, MessageTrackingReporter> getMessageTrackingReporters() {
    return messageTrackingReporters;
  }

  public void setMessageTrackingReporters(Map<String, MessageTrackingReporter> messageTrackingReporters) {
    this.messageTrackingReporters = messageTrackingReporters;
  }
  
  public MessageTrackingReporter getMessageTrackingLogReporter(String name, boolean create) {
    MessageTrackingReporter reporter = messageTrackingReporters.get(name);
    if(reporter == null && create) {
      reporter = new MessageTrackingReporter(name);
      messageTrackingReporters.put(name, reporter);
    }
    return reporter;
  }
}