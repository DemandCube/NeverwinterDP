package com.neverwinterdp.scribengin.dataflow.tool.tracking;

abstract public class TrackingMessageWriter {
  public void onInit(TrackingRegistry registry) throws Exception {
  }
 
  public void onDestroy(TrackingRegistry registry) throws Exception{
  }
  
  abstract public void write(TrackingMessage message) throws Exception ;
}
