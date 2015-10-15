package com.neverwinterdp.scribengin.dataflow.tool.tracking;

abstract public class TrackingMessageReader {
  public void onInit(TrackingRegistry registry) throws Exception {
  }
 
  public void onDestroy(TrackingRegistry registry) throws Exception{
  }
  
  abstract public TrackingMessage next() throws Exception ;
}
