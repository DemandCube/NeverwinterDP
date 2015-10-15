package com.neverwinterdp.scribengin.dataflow.tool.tracking;

import java.util.BitSet;

public class TrackingMessageReportBitSet {
  private TrackingMessageReport report;
 
  private BitSet bitSet ;
  private int duplicatedCount = 0;
  private int numOfBits ;
  private int trackProgress = -1;
  private int logCount ;
  
  public TrackingMessageReportBitSet(String vmId, String chunkId, int expectNumOfMessage) {
    report = new TrackingMessageReport(vmId, chunkId, expectNumOfMessage);
    
    this.numOfBits = expectNumOfMessage ;
    bitSet = new BitSet(expectNumOfMessage) ;
  }
  
  synchronized public int log(int idx) {
    if(idx > numOfBits) {
      throw new RuntimeException("the index is bigger than expect num of bits " + numOfBits);
    }
    if(idx > trackProgress) trackProgress = idx;
    if(bitSet.get(idx)) duplicatedCount++ ;
    bitSet.set(idx, true);
    logCount++ ;
    return logCount;
  }
  
  public TrackingMessageReport getReport() { return this.report; }
  
  synchronized public TrackingMessageReport updateAndGetReport() {
    int lostCount = 0;
    int noLostTo = -1;
    for(int i = 0; i < trackProgress; i++) {
      if(!bitSet.get(i)) {
        if(noLostTo < 0) noLostTo = i;
        lostCount++ ;
      }
    }
    if(noLostTo < 0) noLostTo = trackProgress;
    report.setProgress(trackProgress + 1);
    report.setNoLostTo(noLostTo + 1);
    report.setLostCount(lostCount);
    report.setDuplicatedCount(duplicatedCount);
    return report;
  }
}
