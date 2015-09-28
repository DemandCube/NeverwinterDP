package com.neverwinterdp.scribengin.dataflow.config;

import java.util.Set;

public class OperatorConfig {
  private String      scribe;
  private String      inputStream;
  private Set<String> outputStreams;
  
  public String getScribe() { return scribe; }
  public void setScribe(String scribe) { this.scribe = scribe;}
  
  public String getInputStream() { return inputStream; }
  public void setInputStream(String inputStream) { this.inputStream = inputStream; }
  
  public Set<String> getOutputStreams() { return outputStreams; }
  public void setOutputStreams(Set<String> outputStreams) {
    this.outputStreams = outputStreams;
  }
}
