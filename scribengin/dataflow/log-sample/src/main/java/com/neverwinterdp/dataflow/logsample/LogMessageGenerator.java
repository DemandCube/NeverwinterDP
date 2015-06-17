package com.neverwinterdp.dataflow.logsample;

import java.util.concurrent.atomic.AtomicInteger;

import com.beust.jcommander.Parameter;
import com.neverwinterdp.util.JSONSerializer;

public class LogMessageGenerator {
  private String groupId ;
  @Parameter(names = "--log-generator-message-size", description = "Log message size")
  private int messageSize = 256;
  
  private AtomicInteger seqIdTracker = new AtomicInteger();

  public LogMessageGenerator(String groupId, int messageSize) {
    this.groupId        = groupId;
    this.messageSize = messageSize;
  }
  
  public LogMessage nextMessage() {
    return new LogMessage(groupId, seqIdTracker.incrementAndGet(), messageSize) ;
  }
  
  public String nextMessageAsJson() {
    return JSONSerializer.INSTANCE.toString(nextMessage());
  }
  
  public LogMessageReport getLogMessageReport() {
    LogMessageReport report = new LogMessageReport(groupId, seqIdTracker.get()) ;
    return report ;
  }
}
