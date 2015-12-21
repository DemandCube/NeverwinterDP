package com.neverwinterdp.message;

import java.util.HashMap;
import java.util.Map;

public class MessageTrackingReporter {
  private Map<String, MessageTrackingLogReporter> messageTrackingLogReporters ;
  
  public MessageTrackingReporter() {
  }
  
  public MessageTrackingReporter(boolean init) {
    messageTrackingLogReporters = new HashMap<>();
  }

  public Map<String, MessageTrackingLogReporter> getMessageTrackingLogReporters() {
    return messageTrackingLogReporters;
  }

  public void setMessageTrackingLogReporters(Map<String, MessageTrackingLogReporter> messageTrackingLogReporters) {
    this.messageTrackingLogReporters = messageTrackingLogReporters;
  }
  
  public MessageTrackingLogReporter getMessageTrackingLogReporter(String name, boolean create) {
    MessageTrackingLogReporter reporter = messageTrackingLogReporters.get(name);
    if(reporter == null && create) {
      reporter = new MessageTrackingLogReporter(name);
      messageTrackingLogReporters.put(name, reporter);
    }
    return reporter;
  }
}
